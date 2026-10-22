use arrow::{
    array::{AsArray, BooleanBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder},
    buffer::NullBuffer,
    compute::{concat, take},
};
use arrow_array::{Array, PrimitiveArray, RecordBatch, StringArray, StructArray, array};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::common::{
    DataFusionError, Result, ScalarValue, Statistics, not_impl_err, plan_err,
};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, ExecutionPlan, Partitioning, memory::MemoryStream,
        stream::RecordBatchStreamAdapter,
    },
};
use std::collections::HashMap;
use std::{
    any::Any,
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use crate::functions::MultiHashFunction;
use crate::schemas::window_arrow_struct;
use crate::{make_udf_function, register_functions};
use arrow_array::types::{TimestampNanosecondType, UInt64Type};
use arroyo_operator::operator::Registry;
use arroyo_rpc::grpc::api::{ArroyoExecNode, DebeziumEncodeNode, MemExecNode, arroyo_exec_node};
use arroyo_rpc::{
    TIMESTAMP_FIELD, UPDATING_META_FIELD,
    grpc::api::{DebeziumDecodeNode, arroyo_exec_node::Node},
    updating_meta_field, updating_meta_fields,
};
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};
use prost::Message;
use std::fmt::Debug;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

/// Prove one complete source batch survives this private CDC segment. No
/// repartitioner/coalescer/expander may participate, even behind scalar labels.
pub(crate) fn validate_complete_source_batch(plan: &dyn ExecutionPlan) -> Result<()> {
    if plan.as_any().is::<ArroyoMemExec>() {
        if !plan.children().is_empty() {
            return plan_err!("source batch reader must be a leaf");
        }
        return Ok(());
    }
    let projection = plan
        .as_any()
        .downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>();
    if projection.is_none()
        && !plan
            .as_any()
            .is::<datafusion::physical_plan::filter::FilterExec>()
        && !plan.as_any().is::<DebeziumUnrollingExec>()
    {
        return plan_err!(
            "per-event correction requires batch-preserving Projection/Filter/source unrolling, found {}",
            plan.name()
        );
    }
    let children = plan.children();
    if children.len() != 1 {
        return plan_err!("per-event correction requires one physical batch input");
    }
    if let Some(projection) = projection {
        let input = children[0].schema();
        if let Ok(input_index) = input.index_of(arroyo_rpc::SOURCE_ENVELOPE_FIELD) {
            let output = projection.schema();
            let output_index = output.index_of(arroyo_rpc::SOURCE_ENVELOPE_FIELD)?;
            let column = projection.expr()[output_index]
                .0
                .as_any()
                .downcast_ref::<datafusion::physical_expr::expressions::Column>();
            if column.is_none_or(|column| column.index() != input_index) {
                return plan_err!("per-event correction projection replaces the source envelope");
            }
        }
    }
    validate_complete_source_batch(children[0].as_ref())
}

#[derive(Debug)]
pub struct WindowFunctionUdf {
    signature: Signature,
}

impl Default for WindowFunctionUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for WindowFunctionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "window"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(window_arrow_struct())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = args.args;
        if columns.len() != 2 {
            return plan_err!("window function expected 2 argument, got {}", columns.len());
        }
        // check both columns are of the correct type
        if columns[0].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
            return plan_err!(
                "window function expected first argument to be a timestamp, got {:?}",
                columns[0].data_type()
            );
        }
        if columns[1].data_type() != DataType::Timestamp(TimeUnit::Nanosecond, None) {
            return plan_err!(
                "window function expected second argument to be a timestamp, got {:?}",
                columns[1].data_type()
            );
        }
        let fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "end",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
        ]
        .into();

        match (&columns[0], &columns[1]) {
            (ColumnarValue::Array(start), ColumnarValue::Array(end)) => {
                Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                    fields,
                    vec![start.clone(), end.clone()],
                    None,
                ))))
            }
            (ColumnarValue::Array(start), ColumnarValue::Scalar(end)) => {
                let end = end.to_array_of_size(start.len())?;
                Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                    fields,
                    vec![start.clone(), end],
                    None,
                ))))
            }
            (ColumnarValue::Scalar(start), ColumnarValue::Array(end)) => {
                let start = start.to_array_of_size(end.len())?;
                Ok(ColumnarValue::Array(Arc::new(StructArray::new(
                    fields,
                    vec![start, end.clone()],
                    None,
                ))))
            }
            (ColumnarValue::Scalar(start), ColumnarValue::Scalar(end)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Struct(
                    StructArray::new(fields, vec![start.to_array()?, end.to_array()?], None).into(),
                )))
            }
        }
    }
}

make_udf_function!(WindowFunctionUdf, WINDOW_FUNCTION, window);

#[derive(Debug)]
pub struct ArroyoPhysicalExtensionCodec {
    pub context: DecodingContext,
}

impl Default for ArroyoPhysicalExtensionCodec {
    fn default() -> Self {
        Self {
            context: DecodingContext::None,
        }
    }
}
#[derive(Debug)]
pub enum DecodingContext {
    None,
    Planning,
    SingleLockedBatch(Arc<RwLock<Option<RecordBatch>>>),
    UnboundedBatchStream(Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>),
    /// A finite source for state-backed windows. Producers wait for the
    /// aggregate consumer between pages, including on a hot window key.
    BoundedBatchStream(Arc<RwLock<Option<Receiver<RecordBatch>>>>),
    LockedBatchVec(Arc<RwLock<Vec<RecordBatch>>>),
    LockedJoinPair {
        left: Arc<RwLock<Option<RecordBatch>>>,
        right: Arc<RwLock<Option<RecordBatch>>>,
    },
    LockedJoinStream {
        left: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
        right: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    },
}

pub fn new_registry() -> Registry {
    let mut registry = Registry::default();
    registry.add_udf(window());
    register_functions(&mut registry);
    registry
}

fn make_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        },
    )
}

fn unnest_output_schema(input: &Schema, column: usize) -> Result<Schema> {
    let field = input
        .fields()
        .get(column)
        .ok_or_else(|| DataFusionError::Internal("unnest input index exceeds schema".into()))?;
    let DataType::List(element) = field.data_type() else {
        return Err(DataFusionError::Internal(
            "unnest input column must be a List".into(),
        ));
    };
    let mut fields = input.fields().to_vec();
    fields[column] = Arc::new(element.as_ref().clone().with_name(field.name()));
    Ok(Schema::new_with_metadata(fields, input.metadata().clone()))
}

/// Validate one SQL/JSON kernel at any native expression placement. Callers
/// traverse children; this check does not recursively validate local query plans.
pub(crate) fn validate_json_table_physical_expression(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    input: &Schema,
) -> Result<()> {
    use datafusion::physical_expr::{ScalarFunctionExpr, expressions::Literal};
    if let Some(function) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
        if let Some(regex) = function
            .fun()
            .inner()
            .as_any()
            .downcast_ref::<crate::like_regex::LikeRegexFunc>()
        {
            let fields = function
                .args()
                .iter()
                .map(|arg| arg.return_field(input))
                .collect::<Result<Vec<_>>>()?;
            let literals = function
                .args()
                .iter()
                .map(|arg| arg.as_any().downcast_ref::<Literal>().map(Literal::value))
                .collect::<Vec<_>>();
            let expected = function.fun().return_field_from_args(
                datafusion::logical_expr::ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &literals,
                },
            )?;
            if regex.pattern().is_none()
                || expected.data_type() != function.return_type()
                || expected.is_nullable() != function.nullable()
            {
                return Err(DataFusionError::Plan(
                    "LIKE_REGEX physical return schema or compiled literal mismatch".into(),
                ));
            }
        }
        let expected = match function.name() {
            "json_table_rows" => Some(DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Utf8,
                false,
            )))),
            "json_value" | "json_query" => Some(DataType::Utf8),
            "json_value_boolean" | "json_exists" => Some(DataType::Boolean),
            "json_value_double" => Some(DataType::Float64),
            _ => None,
        };
        if let Some(expected) = expected {
            if function.args().len() != 2 {
                return Err(DataFusionError::Plan(format!(
                    "{} physical decode requires document and literal path",
                    function.name()
                )));
            }
            if function.args()[0].data_type(input)? != DataType::Utf8 {
                return Err(DataFusionError::Plan(format!(
                    "{} physical decode document must be UTF8",
                    function.name()
                )));
            }
            let Some(literal) = function.args()[1].as_any().downcast_ref::<Literal>() else {
                return Err(DataFusionError::Plan(format!(
                    "{} physical decode path must be a non-null UTF8 literal",
                    function.name()
                )));
            };
            let ScalarValue::Utf8(Some(path)) = literal.value() else {
                return Err(DataFusionError::Plan(format!(
                    "{} physical decode path must be a non-null UTF8 literal",
                    function.name()
                )));
            };
            crate::sql_json::path::compile_literal_path(path)?;
            let nullable = match function.name() {
                "json_table_rows" => false,
                "json_exists" => function.args()[0].nullable(input)?,
                _ => true,
            };
            if function.return_type() != &expected || function.nullable() != nullable {
                return Err(DataFusionError::Plan(format!(
                    "{} physical decode return schema/nullability disagrees with kernel",
                    function.name()
                )));
            }
        }
    }
    Ok(())
}

/// Validate the SQL/JSON extraction contracts after native physical decoding.
/// DataFusion's decoder trusts serialized UDF return fields rather than calling
/// return_field_from_args. The standard JSON_TABLE lowering emits these kernels
/// in projections; filters can also contain the shared column expressions.
/// No document, compiled path or schema allocation is retained by this check.
pub fn validate_json_table_execution_plan(plan: &dyn ExecutionPlan) -> Result<()> {
    validate_json_table_plan(plan, false)
}

/// Finite local plans also admit native FROM UNNEST's depth-one options.
/// Ordinary JSON_TABLE plans retain their stricter generated options contract.
pub(crate) fn validate_local_json_table_execution_plan(plan: &dyn ExecutionPlan) -> Result<()> {
    validate_json_table_plan(plan, true)
}

fn validate_json_table_plan(plan: &dyn ExecutionPlan, native_local: bool) -> Result<()> {
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::{filter::FilterExec, projection::ProjectionExec};
    fn expression(expr: &Arc<dyn PhysicalExpr>, input: &Schema) -> Result<()> {
        crate::local_physical::validate_local_expression(expr, input)?;
        validate_json_table_physical_expression(expr, input)?;
        for child in expr.children() {
            expression(child, input)?;
        }
        Ok(())
    }
    if let Some(unnest) = plan.as_any().downcast_ref::<UnnestExec>() {
        if !native_local
            && (!unnest.options().preserve_nulls || !unnest.options().recursions.is_empty())
        {
            return Err(DataFusionError::Plan(
                "JSON_TABLE decoded UNNEST options disagree with generated contract".into(),
            ));
        }
        if unnest.list_column_indices().len() != 1
            || unnest.list_column_indices()[0].depth != 1
            || !unnest.struct_column_indices().is_empty()
            || unnest
                .options()
                .recursions
                .iter()
                .any(|recursion| recursion.depth != 1)
        {
            return Err(DataFusionError::NotImplemented(format!(
                "unnest supports one depth-1 List without recursive options: {:?}; {:?}",
                unnest.list_column_indices(),
                unnest.options()
            )));
        }
        let mut expected = unnest_output_schema(
            unnest.input().schema().as_ref(),
            unnest.list_column_indices()[0].index_in_input_schema,
        )?;
        if let Some(recursion) = unnest.options().recursions.first() {
            let mut fields = expected.fields().to_vec();
            let index = unnest.list_column_indices()[0].index_in_input_schema;
            fields[index] = Arc::new(
                fields[index]
                    .as_ref()
                    .clone()
                    .with_name(&recursion.output_column.name),
            );
            expected = Schema::new_with_metadata(fields, expected.metadata().clone());
        }
        if unnest.schema().as_ref() != &expected {
            return Err(DataFusionError::Plan(
                "unnest decoded output schema disagrees with input".into(),
            ));
        }
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        for (expr, _) in projection.expr() {
            expression(expr, projection.input().schema().as_ref())?;
        }
    }
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        expression(filter.predicate(), filter.input().schema().as_ref())?;
    }
    for input in plan.children() {
        validate_json_table_plan(input.as_ref(), native_local)?;
    }
    Ok(())
}

impl PhysicalExtensionCodec for ArroyoPhysicalExtensionCodec {
    fn try_encode_udf(
        &self,
        node: &datafusion::logical_expr::ScalarUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(regex) = node
            .inner()
            .as_any()
            .downcast_ref::<crate::like_regex::LikeRegexFunc>()
        {
            let pattern = regex.pattern().ok_or_else(|| {
                DataFusionError::Plan(
                    "LIKE_REGEX physical encoding requires a compiled literal".into(),
                )
            })?;
            buf.extend(serde_json::to_vec(pattern).map_err(|error| {
                DataFusionError::Plan(format!("LIKE_REGEX pattern encoding failed: {error}"))
            })?);
            return Ok(());
        }
        if let Some(encoded) = crate::local_physical::encode_local_udf(node, self)? {
            buf.extend(encoded);
        }
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<datafusion::logical_expr::ScalarUDF>> {
        if crate::like_regex::is_reserved_name(name) {
            // JSON can expand one source byte to six encoded bytes. Refuse
            // oversized untrusted metadata before deserializing an owned string.
            let max_payload = crate::sql_json::path::MAX_PATH_LENGTH * 6 + 2;
            if buf.len() > max_payload {
                return Err(DataFusionError::Plan(format!(
                    "LIKE_REGEX physical pattern payload exceeds {max_payload} bytes"
                )));
            }
            let text = std::str::from_utf8(buf).map_err(|_| {
                DataFusionError::Plan("LIKE_REGEX physical pattern is not UTF8".into())
            })?;
            let pattern: String = serde_json::from_str(text).map_err(|error| {
                DataFusionError::Plan(format!(
                    "LIKE_REGEX physical pattern encoding is invalid: {error}"
                ))
            })?;
            return Ok(Arc::new(
                datafusion::logical_expr::ScalarUDF::new_from_impl(
                    crate::like_regex::LikeRegexFunc::new(&pattern)?,
                ),
            ));
        }
        crate::local_physical::decode_local_udf(name, buf, self)?.ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "no local physical codec for scalar function {name}"
            ))
        })
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        _registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let exec: ArroyoExecNode = Message::decode(buf)
            .map_err(|err| DataFusionError::Internal(format!("couldn't deserialize: {err}")))?;

        match exec
            .node
            .ok_or_else(|| DataFusionError::Internal("exec node is empty".to_string()))?
        {
            Node::MemExec(mem_exec) => {
                let schema: Schema = serde_json::from_str(&mem_exec.schema).map_err(|e| {
                    DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}"))
                })?;
                let schema = Arc::new(schema);
                match &self.context {
                    DecodingContext::SingleLockedBatch(single_batch) => Ok(Arc::new(
                        RwLockRecordBatchReader::new(schema, single_batch.clone()),
                    )),
                    DecodingContext::UnboundedBatchStream(unbounded_stream) => Ok(Arc::new(
                        UnboundedRecordBatchReader::new(schema, unbounded_stream.clone()),
                    )),
                    DecodingContext::BoundedBatchStream(bounded_stream) => Ok(Arc::new(
                        BoundedRecordBatchReader::new(schema, bounded_stream.clone()),
                    )),
                    DecodingContext::LockedBatchVec(locked_batches) => Ok(Arc::new(
                        RecordBatchVecReader::new(schema, locked_batches.clone()),
                    )),
                    DecodingContext::Planning => {
                        Ok(Arc::new(ArroyoMemExec::new(mem_exec.table_name, schema)))
                    }
                    DecodingContext::None => Err(DataFusionError::Internal(
                        "Need an internal context to decode".into(),
                    )),
                    DecodingContext::LockedJoinPair { left, right } => {
                        match mem_exec.table_name.as_str() {
                            "left" => {
                                Ok(Arc::new(RwLockRecordBatchReader::new(schema, left.clone())))
                            }
                            "right" => Ok(Arc::new(RwLockRecordBatchReader::new(
                                schema,
                                right.clone(),
                            ))),
                            _ => Err(DataFusionError::Internal(format!(
                                "unknown table name {}",
                                mem_exec.table_name
                            ))),
                        }
                    }
                    DecodingContext::LockedJoinStream { left, right } => {
                        match mem_exec.table_name.as_str() {
                            "left" => Ok(Arc::new(UnboundedRecordBatchReader::new(
                                schema,
                                left.clone(),
                            ))),
                            "right" => Ok(Arc::new(UnboundedRecordBatchReader::new(
                                schema,
                                right.clone(),
                            ))),
                            _ => Err(DataFusionError::Internal(format!(
                                "unknown table name {}",
                                mem_exec.table_name
                            ))),
                        }
                    }
                }
            }
            Node::DebeziumDecode(debezium) => {
                let schema = Arc::new(serde_json::from_str::<Schema>(&debezium.schema).map_err(
                    |e| DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}")),
                )?);
                Ok(Arc::new(DebeziumUnrollingExec {
                    input: inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("no input for debezium node".to_string())
                        })?
                        .clone(),
                    schema: schema.clone(),
                    properties: make_properties(schema),
                    primary_keys: debezium
                        .primary_keys
                        .into_iter()
                        .map(|c| c as usize)
                        .collect(),
                }))
            }
            Node::DebeziumEncode(debezium) => {
                let schema = Arc::new(serde_json::from_str::<Schema>(&debezium.schema).map_err(
                    |e| DataFusionError::Internal(format!("invalid schema in exec codec: {e:?}")),
                )?);
                Ok(Arc::new(ToDebeziumExec::try_new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("no input for debezium node".to_string())
                        })?
                        .clone(),
                    schema,
                )?))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let mut proto = None;

        let mem_table: Option<&ArroyoMemExec> = node.as_any().downcast_ref();
        if let Some(table) = mem_table {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::MemExec(MemExecNode {
                    table_name: table.table_name.clone(),
                    schema: serde_json::to_string(&table.schema).unwrap(),
                })),
            });
        }

        let debezium_decode: Option<&DebeziumUnrollingExec> = node.as_any().downcast_ref();
        if let Some(decode) = debezium_decode {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::DebeziumDecode(DebeziumDecodeNode {
                    schema: serde_json::to_string(&decode.schema).unwrap(),
                    primary_keys: (*decode.primary_keys).iter().map(|c| *c as u64).collect(),
                })),
            });
        }

        let debezium_encode: Option<&ToDebeziumExec> = node.as_any().downcast_ref();
        if let Some(encode) = debezium_encode {
            proto = Some(ArroyoExecNode {
                node: Some(arroyo_exec_node::Node::DebeziumEncode(DebeziumEncodeNode {
                    schema: serde_json::to_string(&encode.schema).unwrap(),
                })),
            });
        }

        if let Some(node) = proto {
            node.encode(buf).map_err(|err| {
                DataFusionError::Internal(format!("couldn't serialize exec node {err}"))
            })?;
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "cannot serialize {node:?}"
            )))
        }
    }
}

/// Identify the concrete one-batch reader installed when an existing MemExec
/// is decoded with `SingleLockedBatch`. Keep its implementation private: route
/// admission must not accept arbitrary leaf plans or rely on a display name.
pub fn is_single_locked_batch_reader(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().is::<RwLockRecordBatchReader>()
}

#[derive(Debug)]
struct RwLockRecordBatchReader {
    schema: SchemaRef,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
    properties: PlanProperties,
}

impl RwLockRecordBatchReader {
    fn new(schema: SchemaRef, locked_batch: Arc<RwLock<Option<RecordBatch>>>) -> Self {
        Self {
            schema: schema.clone(),
            locked_batch,
            properties: make_properties(schema),
        }
    }
}

impl DisplayAs for RwLockRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RW Lock RecordBatchReader")
    }
}

impl ExecutionPlan for RwLockRecordBatchReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
        let result = self
            .locked_batch
            .write()
            .unwrap()
            .take()
            .expect("should have set a record batch before calling execute()");
        Ok(Box::pin(MemoryStream::try_new(
            vec![result],
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> Result<datafusion::common::Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn name(&self) -> &str {
        "rw_lock_reader"
    }
}

#[derive(Debug)]
struct UnboundedRecordBatchReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    properties: PlanProperties,
}

impl UnboundedRecordBatchReader {
    fn new(
        schema: SchemaRef,
        receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_properties(schema),
        }
    }
}

impl DisplayAs for UnboundedRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "unbounded record batch reader")
    }
}

impl ExecutionPlan for UnboundedRecordBatchReader {
    fn name(&self) -> &str {
        "unbounded_reader"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            UnboundedReceiverStream::new(
                self.receiver
                    .write()
                    .unwrap()
                    .take()
                    .expect("unbounded receiver should be present before calling exec. In general, set it and then immediately call execute()"),
            )
            .map(Ok),
        )))
    }

    fn statistics(&self) -> Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct BoundedRecordBatchReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>,
    properties: PlanProperties,
}

impl BoundedRecordBatchReader {
    fn new(schema: SchemaRef, receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_properties(schema),
        }
    }
}

impl DisplayAs for BoundedRecordBatchReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "bounded record batch reader")
    }
}

impl ExecutionPlan for BoundedRecordBatchReader {
    fn name(&self) -> &str {
        "bounded_reader"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let receiver =
            self.receiver.write().unwrap().take().ok_or_else(|| {
                DataFusionError::Execution("bounded window input is missing".into())
            })?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            ReceiverStream::new(receiver).map(Ok),
        )))
    }
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct RecordBatchVecReader {
    schema: SchemaRef,
    receiver: Arc<RwLock<Vec<RecordBatch>>>,
    properties: PlanProperties,
}

impl RecordBatchVecReader {
    fn new(schema: SchemaRef, receiver: Arc<RwLock<Vec<RecordBatch>>>) -> Self {
        Self {
            schema: schema.clone(),
            receiver,
            properties: make_properties(schema),
        }
    }
}

impl DisplayAs for RecordBatchVecReader {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, " record batch vec reader")
    }
}

impl ExecutionPlan for RecordBatchVecReader {
    fn name(&self) -> &str {
        "vec_reader"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
        let memory = MemorySourceConfig::try_new(
            &[mem::take(self.receiver.write().unwrap().as_mut())],
            self.schema.clone(),
            None,
        )?;

        DataSourceExec::new(Arc::new(memory)).execute(partition, context)
    }

    fn statistics(&self) -> datafusion::common::Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ArroyoMemExec {
    pub table_name: String,
    pub schema: SchemaRef,
    properties: PlanProperties,
}

impl DisplayAs for ArroyoMemExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EmptyPartitionStream: schema={}", self.schema)
    }
}

impl ArroyoMemExec {
    pub fn new(table_name: String, schema: SchemaRef) -> Self {
        Self {
            schema: schema.clone(),
            table_name,
            properties: make_properties(schema),
        }
    }
}

impl ExecutionPlan for ArroyoMemExec {
    fn name(&self) -> &str {
        "mem_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("with_new_children is not implemented for mem_exec; should not be called")
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        plan_err!(
            "EmptyPartitionStream cannot be executed, this is only used for physical planning before serialization"
        )
    }

    fn statistics(&self) -> Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct DebeziumUnrollingExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
    primary_keys: Vec<usize>,
}

impl DebeziumUnrollingExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, primary_keys: Vec<usize>) -> Result<Self> {
        let input_schema = input.schema();
        // confirm that the input schema has before, after and op columns, and before and after match
        let before_index = input_schema.index_of("before")?;
        let after_index = input_schema.index_of("after")?;
        let op_index = input_schema.index_of("op")?;
        let _timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let before_type = input_schema.field(before_index).data_type();
        let after_type = input_schema.field(after_index).data_type();
        if before_type != after_type {
            return Err(DataFusionError::Internal(
                "before and after columns must have the same type".to_string(),
            ));
        }
        // check that op is a string
        let op_type = input_schema.field(op_index).data_type();
        if *op_type != DataType::Utf8 {
            return Err(DataFusionError::Internal(
                "op column must be a string".to_string(),
            ));
        }
        // create the output schema
        let DataType::Struct(fields) = before_type else {
            return Err(DataFusionError::Internal(
                "before and after columns must be structs".to_string(),
            ));
        };
        let mut fields = fields.to_vec();
        fields.push(updating_meta_field());
        fields.push(Arc::new(arrow::datatypes::Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )));

        fields.push(Arc::new(arrow::datatypes::Field::new(
            arroyo_rpc::SOURCE_ENVELOPE_FIELD,
            DataType::UInt64,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            input,
            schema: schema.clone(),
            properties: make_properties(schema),
            primary_keys,
        })
    }
}

impl DisplayAs for DebeziumUnrollingExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DebeziumUnrollingExec")
    }
}

impl ExecutionPlan for DebeziumUnrollingExec {
    fn name(&self) -> &str {
        "debezium_unrolling_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DebeziumUnrollingExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(DebeziumUnrollingExec {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            primary_keys: self.primary_keys.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DebeziumUnrollingStream::try_new(
            self.input.execute(partition, context)?,
            self.schema.clone(),
            self.primary_keys.clone(),
        )?))
    }

    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
}

struct DebeziumUnrollingStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    before_index: usize,
    after_index: usize,
    op_index: usize,
    timestamp_index: usize,
    primary_keys: Vec<usize>,
}

impl DebeziumUnrollingStream {
    fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        primary_keys: Vec<usize>,
    ) -> Result<Self> {
        if primary_keys.is_empty() {
            return plan_err!("there must be at least one primary key for a Debezium source");
        }
        let input_schema = input.schema();
        let before_index = input_schema.index_of("before")?;
        let after_index = input_schema.index_of("after")?;
        let op_index = input_schema.index_of("op")?;
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;

        Ok(Self {
            input,
            schema,
            before_index,
            after_index,
            op_index,
            timestamp_index,
            primary_keys,
        })
    }
    fn unroll_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let before = batch.column(self.before_index).as_ref();
        let after = batch.column(self.after_index).as_ref();
        let op = batch
            .column(self.op_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("op column is not a string".to_string()))?;

        let timestamp = batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<array::TimestampNanosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("timestamp column is not a timestamp".to_string())
            })?;

        let num_rows = batch.num_rows();
        let combined_array = concat(&[before, after])?;
        let mut take_indices = UInt32Builder::with_capacity(num_rows);
        let mut is_retract_builder = BooleanBuilder::with_capacity(num_rows);

        let mut envelope_builder = UInt64Builder::with_capacity(2 * num_rows);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(2 * num_rows);
        for i in 0..num_rows {
            let op = op.value(i);
            match op {
                "c" | "r" => {
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(timestamp.value(i));
                    envelope_builder.append_value(i as u64);
                }
                "u" => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(timestamp.value(i));
                    envelope_builder.append_value(i as u64);
                    take_indices.append_value((i + num_rows) as u32);
                    is_retract_builder.append_value(false);
                    timestamp_builder.append_value(timestamp.value(i));
                    envelope_builder.append_value(i as u64);
                }
                "d" => {
                    take_indices.append_value(i as u32);
                    is_retract_builder.append_value(true);
                    timestamp_builder.append_value(timestamp.value(i));
                    envelope_builder.append_value(i as u64);
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "unexpected op value: {op}"
                    )));
                }
            }
        }
        let take_indices = take_indices.finish();
        let unrolled_array = take(&combined_array, &take_indices, None)?;

        let mut columns = unrolled_array.as_struct().columns().to_vec();

        let hash = MultiHashFunction::default().invoke(
            &self
                .primary_keys
                .iter()
                .map(|i| ColumnarValue::Array(columns[*i].clone()))
                .collect::<Vec<_>>(),
        )?;

        let ids = hash.into_array(num_rows)?;

        let meta = StructArray::try_new(
            updating_meta_fields(),
            vec![Arc::new(is_retract_builder.finish()), ids],
            None,
        )?;
        columns.push(Arc::new(meta));
        columns.push(Arc::new(timestamp_builder.finish()));
        columns.push(Arc::new(envelope_builder.finish()));
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for DebeziumUnrollingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result =
            ready!(self.input.poll_next_unpin(cx)).map(|result| self.unroll_batch(&result?));
        Poll::Ready(result)
    }
}

impl RecordBatchStream for DebeziumUnrollingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub struct ToDebeziumExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

// Physical nullability is an estimate; the declared SQL result controls the
// CDC schema. Types and field attributes must agree, while runtime array
// reconstruction checks actual nulls against declared nested fields.
fn debezium_type_compatible(actual: &DataType, declared: &DataType) -> bool {
    match (actual, declared) {
        (DataType::Struct(actual), DataType::Struct(declared)) => {
            actual.len() == declared.len()
                && actual
                    .iter()
                    .zip(declared.iter())
                    .all(|(actual, declared)| debezium_field_compatible(actual, declared))
        }
        (DataType::List(actual), DataType::List(declared))
        | (DataType::LargeList(actual), DataType::LargeList(declared)) => {
            debezium_field_compatible(actual, declared)
        }
        (
            DataType::FixedSizeList(actual, actual_len),
            DataType::FixedSizeList(declared, declared_len),
        ) => actual_len == declared_len && debezium_field_compatible(actual, declared),
        _ => actual == declared,
    }
}

fn debezium_value_metadata(field: &arrow_schema::Field) -> std::collections::BTreeMap<&str, &str> {
    field
        .metadata()
        .iter()
        .filter(|(key, _)| key.as_str() != crate::rewriters::EVENT_CLOCK_PROVENANCE)
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

// Arrow 55 exposes dictionary identity only through this deprecated accessor;
// dropping the check would silently relabel dictionary values at the CDC sink.
#[allow(deprecated)]
fn debezium_field_compatible(actual: &arrow_schema::Field, declared: &arrow_schema::Field) -> bool {
    actual.name() == declared.name()
        // Event-clock provenance is a planner annotation. DataFusion physical
        // projection aliases do not carry alias metadata into Arrow fields;
        // it has no effect on the CDC payload's value representation.
        && debezium_value_metadata(actual) == debezium_value_metadata(declared)
        && actual.dict_id() == declared.dict_id()
        && actual.dict_is_ordered() == declared.dict_is_ordered()
        && debezium_type_compatible(actual.data_type(), declared.data_type())
}

fn debezium_relabel_array(array: &Arc<dyn Array>, declared: &DataType) -> Result<Arc<dyn Array>> {
    if !debezium_type_compatible(array.data_type(), declared) {
        return plan_err!("physical Debezium value type conflicts with declared SQL schema");
    }
    match declared {
        DataType::Struct(fields) => {
            let actual = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan("physical Debezium struct has wrong array type".into())
                })?;
            let columns = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    debezium_relabel_array(actual.column(index), field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                actual.nulls().cloned(),
                actual.len(),
            )?))
        }
        DataType::List(field) => {
            let actual = array
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan("physical Debezium list has wrong array type".into())
                })?;
            Ok(Arc::new(arrow_array::ListArray::try_new(
                field.clone(),
                actual.offsets().clone(),
                debezium_relabel_array(actual.values(), field.data_type())?,
                actual.nulls().cloned(),
            )?))
        }
        DataType::LargeList(field) => {
            let actual = array
                .as_any()
                .downcast_ref::<arrow_array::LargeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan("physical Debezium list has wrong array type".into())
                })?;
            Ok(Arc::new(arrow_array::LargeListArray::try_new(
                field.clone(),
                actual.offsets().clone(),
                debezium_relabel_array(actual.values(), field.data_type())?,
                actual.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(field, size) => {
            let actual = array
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan("physical Debezium list has wrong array type".into())
                })?;
            Ok(Arc::new(arrow_array::FixedSizeListArray::try_new(
                field.clone(),
                *size,
                debezium_relabel_array(actual.values(), field.data_type())?,
                actual.nulls().cloned(),
            )?))
        }
        _ if array.data_type() == declared => Ok(array.clone()),
        _ => plan_err!("physical Debezium value type conflicts with declared SQL schema"),
    }
}

impl ToDebeziumExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, declared_schema: SchemaRef) -> Result<Self> {
        let input_schema = input.schema();
        let timestamp_index = input_schema.index_of(TIMESTAMP_FIELD)?;
        let struct_fields: Vec<_> = input_schema
            .fields()
            .into_iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if field.name() == UPDATING_META_FIELD || index == timestamp_index {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .collect();
        let struct_data_type = DataType::Struct(struct_fields.into());
        let before_field = Arc::new(arrow::datatypes::Field::new(
            "before",
            struct_data_type.clone(),
            true,
        ));
        let after_field = Arc::new(arrow::datatypes::Field::new(
            "after",
            struct_data_type,
            true,
        ));
        let op_field = Arc::new(arrow::datatypes::Field::new("op", DataType::Utf8, false));
        let timestamp_field = Arc::new(input_schema.field(timestamp_index).clone());

        let output_schema = Arc::new(Schema::new(vec![
            before_field,
            after_field,
            op_field,
            timestamp_field,
        ]));

        if output_schema.fields().len() != declared_schema.fields().len()
            || !output_schema
                .fields()
                .iter()
                .zip(declared_schema.fields().iter())
                .all(|(actual, declared)| debezium_field_compatible(actual, declared))
        {
            return plan_err!("physical Debezium schema conflicts with declared SQL schema");
        }

        Ok(Self {
            input,
            schema: declared_schema.clone(),
            properties: make_properties(declared_schema),
        })
    }
}

impl DisplayAs for ToDebeziumExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ToDebeziumExec")
    }
}

impl ExecutionPlan for ToDebeziumExec {
    fn name(&self) -> &str {
        "to_debezium_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ToDebeziumExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(ToDebeziumExec::try_new(
            children[0].clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let updating_meta_index = self.input.schema().index_of(UPDATING_META_FIELD).ok();
        let timestamp_index = self.input.schema().index_of(TIMESTAMP_FIELD)?;
        let struct_projection = (0..self.input.schema().fields().len())
            .filter(|index| {
                updating_meta_index
                    .map(|is_retract_index| *index != is_retract_index)
                    .unwrap_or(true)
                    && *index != timestamp_index
            })
            .collect();

        Ok(Box::pin(ToDebeziumStream {
            input: self.input.execute(partition, context)?,
            schema: self.schema.clone(),
            updating_meta_index,
            timestamp_index,
            struct_projection,
        }))
    }

    fn reset(&self) -> Result<()> {
        self.input.reset()
    }
}

struct ToDebeziumStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    updating_meta_index: Option<usize>,
    timestamp_index: usize,
    struct_projection: Vec<usize>,
}

impl ToDebeziumStream {
    fn as_debezium_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let physical_value = batch.project(&self.struct_projection)?;
        let DataType::Struct(declared_fields) = self.schema.field(0).data_type() else {
            return plan_err!("declared Debezium before field is not a struct");
        };
        if physical_value.num_columns() != declared_fields.len() {
            return plan_err!("physical Debezium value width conflicts with declared SQL schema");
        }
        let columns = physical_value
            .columns()
            .iter()
            .zip(declared_fields.iter())
            .enumerate()
            .map(|(index, (column, field))| {
                if !debezium_field_compatible(physical_value.schema().field(index), field) {
                    return plan_err!("physical Debezium value conflicts with declared SQL schema");
                }
                debezium_relabel_array(column, field.data_type())
            })
            .collect::<Result<Vec<_>>>()?;
        let value_struct =
            RecordBatch::try_new(Arc::new(Schema::new(declared_fields.clone())), columns)?;
        let timestamps = batch
            .column(self.timestamp_index)
            .as_primitive::<TimestampNanosecondType>();

        let columns: Vec<Arc<dyn Array>> = if let Some(metadata_index) = self.updating_meta_index {
            let metadata = batch
                .column(metadata_index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Invalid type for updating_meta column".to_string())
                })?;

            let is_retract = metadata.column(0).as_boolean();

            let id = metadata.column(1).as_fixed_size_binary();

            // (first idx, last idx, first is create, last is create, latest time)
            let mut id_map: HashMap<&[u8], (usize, usize, bool, bool, i64)> = HashMap::new();
            let mut order = vec![];
            for i in 0..batch.num_rows() {
                let row_id = id.value(i);
                let is_create = !is_retract.value(i);
                let timestamp = timestamps.value(i);

                id_map
                    .entry(row_id)
                    .and_modify(|e| {
                        e.1 = i;
                        e.3 = is_create;
                        e.4 = e.4.max(timestamp);
                    })
                    .or_insert_with(|| {
                        order.push(row_id);
                        (i, i, is_create, is_create, timestamp)
                    });
            }

            // scenarios:
            // 1. Create -- value index is last
            //    -- create, (retract, create)*
            // 2. Delete -- value index is first
            //    -- (retract,create)*,retract
            // 3. Update -- value index is last
            //    -- (retract,create)*

            let mut before = Vec::with_capacity(id_map.len());
            let mut after = Vec::with_capacity(id_map.len());
            let mut op = Vec::with_capacity(id_map.len());
            let mut ts = TimestampNanosecondBuilder::with_capacity(id_map.len());

            for row_id in order {
                let (first_idx, last_idx, first_is_create, last_is_create, timestamp) =
                    id_map.get(row_id).unwrap();

                if *first_is_create && *last_is_create {
                    // create case
                    // sequence: create, (retract, create)*
                    before.push(None);
                    after.push(Some(*last_idx));
                    op.push("c");
                } else if !(*first_is_create) && !(*last_is_create) {
                    // delete case
                    // sequence: (retract,create)*, retract
                    before.push(Some(*first_idx));
                    after.push(None);
                    op.push("d");
                } else if !(*first_is_create) && *last_is_create {
                    // update case
                    // sequence: (retract,create)*
                    before.push(Some(*first_idx));
                    after.push(Some(*last_idx));
                    op.push("u");
                } else {
                    // no-op case
                    // sequence: (create,retract)*
                    continue;
                }

                ts.append_value(*timestamp);
            }

            let before_array = Self::create_output_array(&value_struct, &before)?;
            let after_array = Self::create_output_array(&value_struct, &after)?;
            let op_array = StringArray::from(op);

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(op_array),
                Arc::new(ts.finish()),
            ]
        } else {
            // Append-only
            let after_array = StructArray::try_new(
                value_struct.schema().fields().clone(),
                value_struct.columns().to_vec(),
                None,
            )?;

            let before_array = StructArray::new_null(
                value_struct.schema().fields().clone(),
                value_struct.num_rows(),
            );

            vec![
                Arc::new(before_array),
                Arc::new(after_array),
                Arc::new(StringArray::from(vec!["c"; value_struct.num_rows()])),
                batch.column(self.timestamp_index).clone(),
            ]
        };

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn create_output_array(
        value_struct: &RecordBatch,
        indices: &[Option<usize>],
    ) -> Result<StructArray> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(value_struct.num_columns());
        for col in value_struct.columns() {
            let new_array = arrow::compute::take(
                col.as_ref(),
                &indices
                    .iter()
                    .map(|&idx| idx.map(|i| i as u64))
                    .collect::<PrimitiveArray<UInt64Type>>(),
                None,
            )?;
            arrays.push(new_array);
        }

        Ok(StructArray::try_new(
            value_struct.schema().fields().clone(),
            arrays,
            Some(NullBuffer::from(
                indices.iter().map(|&idx| idx.is_some()).collect::<Vec<_>>(),
            )),
        )?)
    }
}

impl Stream for ToDebeziumStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result =
            ready!(self.input.poll_next_unpin(cx)).map(|result| self.as_debezium_batch(&result?));
        Poll::Ready(result)
    }
}

impl RecordBatchStream for ToDebeziumStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod debezium_declared_schema_tests {
    use super::*;
    use arrow::array::new_empty_array;
    use arrow_schema::Field;
    use std::collections::HashMap;

    #[test]
    fn complete_source_batch_whitelist_checks_actual_nodes_and_marker_ordinals() {
        use datafusion::physical_expr::expressions::{Column, Literal};
        use datafusion::physical_plan::{
            coalesce_batches::CoalesceBatchesExec, projection::ProjectionExec,
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            arroyo_rpc::SOURCE_ENVELOPE_FIELD,
            DataType::UInt64,
            false,
        )]));
        let input = Arc::new(ArroyoMemExec::new("batch".into(), schema));
        let valid = ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new(arroyo_rpc::SOURCE_ENVELOPE_FIELD, 0)),
                arroyo_rpc::SOURCE_ENVELOPE_FIELD.into(),
            )],
            input.clone(),
        )
        .unwrap();
        validate_complete_source_batch(&valid).unwrap();
        let replaced = ProjectionExec::try_new(
            vec![(
                Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
                arroyo_rpc::SOURCE_ENVELOPE_FIELD.into(),
            )],
            input.clone(),
        )
        .unwrap();
        assert!(
            validate_complete_source_batch(&replaced)
                .unwrap_err()
                .to_string()
                .contains("replaces the source envelope")
        );
        let dropped = ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new(arroyo_rpc::SOURCE_ENVELOPE_FIELD, 0)),
                "other".into(),
            )],
            input.clone(),
        )
        .unwrap();
        assert!(validate_complete_source_batch(&dropped).is_err());
        let coalesced = CoalesceBatchesExec::new(input, 8);
        assert!(
            validate_complete_source_batch(&coalesced)
                .unwrap_err()
                .to_string()
                .contains("batch-preserving")
        );
    }

    #[test]
    fn source_unrolling_marks_envelopes_independently_of_primary_key() {
        let fields = arrow_schema::Fields::from(vec![Field::new("id", DataType::Int64, false)]);
        let before = StructArray::new(
            fields.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![7, 7, 7]))],
            None,
        );
        let after = StructArray::new(
            fields.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![7, 7, 7]))],
            None,
        );
        let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("before", DataType::Struct(fields.clone()), true),
            Field::new("after", DataType::Struct(fields.clone()), true),
            Field::new("op", DataType::Utf8, false),
            Field::new(TIMESTAMP_FIELD, timestamp_type.clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(before),
                Arc::new(after),
                Arc::new(StringArray::from(vec!["u", "d", "c"])),
                Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
                    10, 11, 12,
                ])),
            ],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            fields[0].clone(),
            updating_meta_field(),
            Arc::new(Field::new(TIMESTAMP_FIELD, timestamp_type, false)),
            Arc::new(Field::new(
                arroyo_rpc::SOURCE_ENVELOPE_FIELD,
                DataType::UInt64,
                false,
            )),
        ]));
        // Exercise the physical constructor used by the extension planner;
        // constructing only the stream can hide logical/physical schema drift.
        let physical = DebeziumUnrollingExec::try_new(
            Arc::new(ArroyoMemExec::new("source".into(), input_schema.clone())),
            vec![0],
        )
        .unwrap();
        assert_eq!(physical.schema(), schema);
        let input = Box::pin(MemoryStream::try_new(vec![], input_schema, None).unwrap());
        let stream = DebeziumUnrollingStream::try_new(input, physical.schema(), vec![0]).unwrap();
        let result = stream.unroll_batch(&batch).unwrap();
        let marker = result
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(marker.values().as_ref(), &[0, 0, 1, 2]);
        let meta = result.column(1).as_struct();
        let retract = meta.column(0).as_boolean();
        assert_eq!(
            retract.values().iter().collect::<Vec<_>>(),
            vec![true, false, true, false]
        );
        let ids = meta.column(1).as_fixed_size_binary();
        assert!(ids.iter().all(|id| id == Some(ids.value(0))));
    }

    #[test]
    fn logical_clock_provenance_does_not_change_debezium_value_type() {
        let plain = Field::new("clock", DataType::Date32, false);
        let annotated = plain
            .clone()
            .with_metadata(std::collections::HashMap::from([(
                crate::rewriters::EVENT_CLOCK_PROVENANCE.to_string(),
                "trigger".to_string(),
            )]));
        assert!(debezium_field_compatible(&plain, &annotated));
        assert!(debezium_field_compatible(&annotated, &plain));
        let value_metadata = plain
            .clone()
            .with_metadata(std::collections::HashMap::from([(
                "value_metadata".to_string(),
                "required".to_string(),
            )]));
        assert!(!debezium_field_compatible(&plain, &value_metadata));
    }

    #[test]
    #[allow(deprecated)]
    fn physical_nested_nonnull_refinement_keeps_declared_nullable_cdc_type() {
        let physical = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("label", DataType::Utf8, false))].into()),
            false,
        )));
        let declared = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("label", DataType::Utf8, true))].into()),
            true,
        )));
        assert!(debezium_type_compatible(&physical, &declared));
        assert!(debezium_type_compatible(&declared, &physical));
        let physical_array = new_empty_array(&physical);
        let relabeled = debezium_relabel_array(&physical_array, &declared).unwrap();
        assert_eq!(relabeled.data_type(), &declared);
        assert_eq!(
            debezium_relabel_array(&relabeled, &physical)
                .unwrap()
                .data_type(),
            &physical
        );
        assert!(!debezium_type_compatible(
            &physical,
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        ));
        let metadata = HashMap::from([("semantic".to_string(), "special".to_string())]);
        assert!(!debezium_field_compatible(
            &Field::new("value", physical.clone(), false).with_metadata(metadata),
            &Field::new("value", declared.clone(), true),
        ));
        assert!(!debezium_field_compatible(
            &Field::new_dict(
                "value",
                DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
                false,
                7,
                true,
            ),
            &Field::new_dict(
                "value",
                DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
                true,
                8,
                true,
            ),
        ));
    }

    #[test]
    fn declared_nonnull_struct_child_rejects_only_unmasked_physical_nulls() {
        let physical_fields = vec![Arc::new(Field::new("value", DataType::Utf8, true))].into();
        let declared =
            DataType::Struct(vec![Arc::new(Field::new("value", DataType::Utf8, false))].into());
        let child = Arc::new(StringArray::from(vec![None::<&str>])) as Arc<dyn Array>;
        let unmasked = Arc::new(StructArray::try_new(physical_fields, vec![child], None).unwrap())
            as Arc<dyn Array>;
        assert!(debezium_relabel_array(&unmasked, &declared).is_err());

        let masked = Arc::new(
            StructArray::try_new(
                vec![Arc::new(Field::new("value", DataType::Utf8, true))].into(),
                vec![Arc::new(StringArray::from(vec![None::<&str>]))],
                Some(NullBuffer::new_null(1)),
            )
            .unwrap(),
        ) as Arc<dyn Array>;
        assert_eq!(
            debezium_relabel_array(&masked, &declared)
                .unwrap()
                .data_type(),
            &declared
        );
    }
}
