//! Configured-backend SQL SESSION owner. The existing DataFusion aggregate
//! remains the final computation; only retained input and scheduling move to
//! the bounded live-state table.
use super::{
    session_accumulator::SessionAccumulatorPlan,
    session_store::{AdmittedSessionState, SessionMeta, SessionStore},
};
use anyhow::{Context, Result, ensure};
use arrow::compute::take;
use arrow_array::{
    Array, PrimitiveArray, RecordBatch, StructArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array, types::TimestampNanosecondType,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use arroyo_operator::{
    context::{Collector, OperatorContext},
    operator::Registry,
};
use arroyo_planner::{
    aggregate_capability::{
        AggregateCapability, aggregate_descriptor, classify_aggregate, is_extremum,
        logical_payload_bytes,
    },
    grouped_selector::{ORDINAL_FIELD, SelectorBudget},
    grouped_selector_udaf::{bind_window_budget, classify},
    physical::{ArroyoPhysicalExtensionCodec, DecodingContext},
    schemas::window_arrow_struct,
};
use arroyo_rpc::{
    Converter,
    config::{WindowStateConfig, config},
    df::ArroyoSchema,
    grpc::{
        api,
        rpc::{DiskKeyedTableConfig, TableConfig, TableEnum},
    },
};
use arroyo_state::live::worker::{
    ConfiguredBackendOwner, configured_worker_resources, construct_configured_backend,
};
use arroyo_types::to_nanos;
use datafusion::execution::{TaskContext, runtime_env::RuntimeEnvBuilder};
use datafusion::physical_plan::{ExecutionPlan, aggregates::AggregateExec};
use datafusion::prelude::SessionContext;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::{StreamExt, try_join};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::sync::mpsc::{Receiver, channel};

const TABLE: &str = "n";

fn output_allowance(
    schema: &Schema,
    output_fields: usize,
    limits: WindowStateConfig,
) -> Result<usize> {
    let variable_fields = schema
        .fields()
        .iter()
        .filter(|field| {
            !matches!(
                field.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(_, _)
                    | DataType::Duration(_)
                    | DataType::Interval(_)
            )
        })
        .count();
    limits
        .partial_bytes
        .checked_mul(variable_fields)
        .and_then(|bytes| bytes.checked_add(limits.key_bytes))
        .and_then(|bytes| bytes.checked_add(output_fields.checked_mul(4096)?))
        .context("native SESSION output admission overflow")
}

fn retained_input_schema(plan: &Arc<dyn ExecutionPlan>) -> Option<arrow_schema::SchemaRef> {
    if plan.name() == "bounded_reader" {
        return Some(plan.schema());
    }
    plan.children()
        .iter()
        .find_map(|child| retained_input_schema(child))
}

fn normalize_collection_plan(
    finish: Arc<dyn ExecutionPlan>,
    input_schema: &arrow_schema::SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    let aggregate = finish
        .as_any()
        .downcast_ref::<AggregateExec>()
        .context("native SESSION final plan must be a physical aggregate")?;
    let original = aggregate
        .input()
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap_or(aggregate);
    let linear = original
        .aggr_expr()
        .iter()
        .map(|expression| classify_aggregate(expression, input_schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?
        .iter()
        .any(|capability| matches!(capability, Some(AggregateCapability::LinearReplay { .. })));
    let collections = linear
        || aggregate.aggr_expr().iter().any(|expression| {
            expression
                .fun()
                .inner()
                .as_any()
                .is::<datafusion::functions_aggregate::array_agg::ArrayAgg>()
        });
    if !collections || *aggregate.mode() == AggregateMode::Single {
        return Ok(finish);
    }
    ensure!(
        *aggregate.mode() == AggregateMode::Final && aggregate.group_expr().expr().is_empty(),
        "native SESSION collection final plan must have one ungrouped final aggregate"
    );
    let partial = aggregate
        .input()
        .as_any()
        .downcast_ref::<AggregateExec>()
        .context("native SESSION collection final input must be a partial aggregate")?;
    ensure!(
        aggregate.aggr_expr().len() == partial.aggr_expr().len()
            && aggregate
                .aggr_expr()
                .iter()
                .zip(partial.aggr_expr())
                .all(|(final_expr, partial_expr)| final_expr.fun().name()
                    == partial_expr.fun().name()
                    && final_expr.is_distinct() == partial_expr.is_distinct()),
        "native SESSION collection partial/final aggregate layout changed"
    );
    ensure!(
        *partial.mode() == AggregateMode::Partial
            && partial.group_expr().expr().is_empty()
            && partial.input().name() == "bounded_reader"
            && partial.input().children().is_empty()
            && partial.input().schema() == *input_schema,
        "native SESSION collection partial input must read retained rows directly"
    );
    // SESSION owns the entire group. Compute once from the original expressions
    // instead of building a complete collection in both partial and final
    // accumulators. Final-mode filters are inert in DataFusion, even when the
    // planner retains them in its metadata; preserve the effective partial
    // filters, order, aliases and the public output schema.
    let single = AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        partial.aggr_expr().to_vec(),
        partial.filter_expr().to_vec(),
        partial.input().clone(),
        input_schema.clone(),
    )?;
    ensure!(
        single.schema() == finish.schema(),
        "native SESSION collection normalization changed output schema"
    );
    Ok(Arc::new(single))
}

fn validate_aggregates(
    finish: &dyn ExecutionPlan,
    input_schema: &arrow_schema::SchemaRef,
) -> Result<()> {
    let aggregate = finish
        .as_any()
        .downcast_ref::<AggregateExec>()
        .context("native SESSION final plan must be a physical aggregate")?;
    ensure!(
        aggregate.group_expr().expr().is_empty(),
        "native SESSION final aggregate must consume one group at a time"
    );
    for (index, expression) in aggregate.aggr_expr().iter().enumerate() {
        let grouped_selector = classify(expression.fun()).is_some();
        let capability = classify_aggregate(expression, input_schema)?;
        let collection = expression
            .fun()
            .inner()
            .as_any()
            .is::<datafusion::functions_aggregate::array_agg::ArrayAgg>();
        if collection {
            ensure!(
                aggregate.input().name() == "bounded_reader"
                    && aggregate.input().children().is_empty()
                    && aggregate.input().schema() == *input_schema,
                "native SESSION ARRAY_AGG requires direct retained-row input without a final-plan projection"
            );
            ensure!(
                expression.expressions().len() == 1
                    && expression.expressions()[0]
                        .as_any()
                        .is::<datafusion::physical_expr::expressions::Column>()
                    && expression
                        .order_bys()
                        .is_none_or(|order| order.iter().all(|sort| {
                            sort.expr
                                .as_any()
                                .is::<datafusion::physical_expr::expressions::Column>()
                        })),
                "native SESSION ARRAY_AGG requires direct source columns for value and ordering expressions"
            );
            ensure!(
                aggregate.filter_expr()[index]
                    .as_ref()
                    .is_none_or(|filter| filter
                        .as_any()
                        .is::<datafusion::physical_expr::expressions::Column>()),
                "native SESSION ARRAY_AGG requires a direct Boolean source column for FILTER"
            );
        }
        ensure!(
            capability.is_some()
                || (!expression.is_distinct()
                    && (is_extremum(expression) || grouped_selector || collection)),
            "native SESSION supports non-distinct COUNT/SUM/AVG/MIN/MAX shared grouped FIRST_VALUE/LAST_VALUE and ARRAY_AGG; other aggregates require bounded output admission"
        );
    }
    Ok(())
}

pub(crate) struct NativeSession {
    gap: i64,
    input_schema: Arc<ArroyoSchema>,
    output_schema: Arc<ArroyoSchema>,
    window_field: FieldRef,
    window_index: usize,
    finish: Arc<dyn ExecutionPlan>,
    finish_receiver: Arc<RwLock<Option<Receiver<RecordBatch>>>>,
    limits: WindowStateConfig,
    task_context: Arc<TaskContext>,
    acknowledged_closure: Option<(Vec<u8>, SessionMeta)>,
    identity: Vec<u8>,
    store: Option<SessionStore>,
    incremental: Option<SessionAccumulatorPlan>,
}

impl NativeSession {
    pub fn new(
        config: &api::SessionWindowAggregateOperator,
        registry: Arc<Registry>,
        limits: WindowStateConfig,
    ) -> Result<Self> {
        limits.validate()?;
        ensure!(config.gap_micros > 0, "native SESSION gap must be positive");
        let gap = i64::try_from(config.gap_micros)?
            .checked_mul(1_000)
            .context("native SESSION gap overflow")?;
        let input_schema: ArroyoSchema = config
            .input_schema
            .clone()
            .context("native SESSION input schema missing")?
            .try_into()?;
        let receiver = Arc::new(RwLock::new(None));
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::BoundedBatchStream(receiver.clone()),
        };
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let finish = PhysicalPlanNode::decode(config.final_aggregation_plan.as_slice())?
            .try_into_physical_plan(registry.as_ref(), &runtime, &codec)?;
        let finish = bind_window_budget(
            finish,
            SelectorBudget {
                key_bytes: limits.key_bytes,
                value_bytes: limits.partial_bytes,
                decoded_bytes: limits
                    .partial_bytes
                    .checked_mul(3)
                    .context("SESSION selector budget overflow")?,
            },
        )?;
        let reader_schema =
            retained_input_schema(&finish).context("native SESSION retained reader missing")?;
        let finish = normalize_collection_plan(finish, &reader_schema)?;
        arroyo_planner::physical::validate_session_result_plan(
            finish.as_ref(),
            &input_schema.schema,
        )?;
        validate_aggregates(finish.as_ref(), &reader_schema)?;
        let output_schema: ArroyoSchema = config
            .output_schema
            .clone()
            .context("native SESSION output descriptor missing")?
            .try_into()?;
        let expected = arroyo_rpc::df::session_output_schema(
            &input_schema,
            finish.schema().as_ref(),
            &config.window_field_name,
            usize::try_from(config.window_index)?,
            &output_schema,
        )?;
        ensure!(
            output_schema == expected,
            "native SESSION output descriptor differs from physical layout"
        );

        let mut incremental = None;
        if let Some(resources) = configured_worker_resources()? {
            // Construction-only admission: classification/template creation may
            // allocate bounded Arrow metadata, never event history.
            {
                // A transient busy pool must not change the persisted execution
                // mode. Only deterministic capacity/eligibility selects fallback.
                let _permit =
                    resources.try_decoded_value(limits.partial_bytes.saturating_mul(3))?;
                if let Some(plan) =
                    SessionAccumulatorPlan::try_new(&finish, &input_schema.schema, limits)?
                    && SessionStore::supports_incremental(
                        limits,
                        resources.config(),
                        &plan.layout,
                        arroyo_state::live::encoding::encoded_namespace_size(
                            &arroyo_state::live::StateNamespace {
                                ownership: arroyo_state::live::Ownership::PartitionLocal {
                                    subtask: 0,
                                    parallelism: 1,
                                },
                                table: TABLE.as_bytes().to_vec(),
                            },
                        )?,
                    )
                {
                    incremental = Some(plan);
                }
            }
        }
        let mut identity = Sha256::new();
        if let Some(plan) = &incremental {
            identity.update(b"streamr.native-session-incremental.v1");
            identity.update(&plan.layout.identity);
        } else {
            identity.update(b"streamr.native-session-raw.v2");
        }
        identity.update(gap.to_be_bytes());
        identity.update(u64::try_from(config.final_aggregation_plan.len())?.to_be_bytes());
        identity.update(&config.final_aggregation_plan);
        identity.update(serde_json::to_vec(input_schema.schema.as_ref())?);
        identity.update(config.window_field_name.as_bytes());
        identity.update(config.window_index.to_be_bytes());
        Ok(Self {
            gap,
            input_schema: Arc::new(input_schema),
            output_schema: Arc::new(output_schema),
            window_field: Arc::new(Field::new(
                &config.window_field_name,
                window_arrow_struct(),
                true,
            )),
            window_index: config.window_index as usize,
            finish,
            finish_receiver: receiver,
            limits,
            task_context: SessionContext::new_with_config_rt(Default::default(), runtime)
                .task_ctx(),
            acknowledged_closure: None,
            identity: identity.finalize().to_vec(),
            store: None,
            incremental,
        })
    }

    pub fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::from([(
            TABLE.to_owned(),
            TableConfig {
                table_type: TableEnum::DiskKeyedMap.into(),
                state_version: 2,
                config: DiskKeyedTableConfig {
                    table_name: TABLE.into(),
                    encoding_version: 1,
                    schema_identity: self.identity.clone(),
                }
                .encode_to_vec(),
            },
        )])
    }

    pub async fn on_start(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        ensure!(
            ctx.in_schemas.len() == 1
                && ctx.in_schemas[0] == self.input_schema
                && ctx.out_schema.as_ref() == Some(&self.output_schema),
            "native SESSION context schemas differ from validated descriptor"
        );
        ensure!(
            ctx.task_info.parallelism == 1 && ctx.task_info.task_index == 0,
            "native SESSION requires one serial owner for its keyed state"
        );
        let worker = config();
        worker.worker.validate_sql_state()?;
        let resources = configured_worker_resources()?
            .context("native SESSION requires worker.live-state-resources")?;
        let generation = match ctx.task_info.checkpoint_file_path_layout {
            arroyo_types::CheckpointFilePathLayout::Protocol { generation, .. } => generation,
            _ => 0,
        };
        let backend = construct_configured_backend(
            ConfiguredBackendOwner {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask: ctx.task_info.task_index,
                generation,
            },
            self.limits.max_resident_bytes,
            resources.clone(),
        )
        .await?;
        let table = ctx
            .table_manager
            .register_live_table(TABLE, backend.clone())
            .await?;
        self.store = Some(if let Some(plan) = &self.incremental {
            SessionStore::new_incremental(
                backend,
                table,
                resources,
                self.input_schema.schema.clone(),
                self.limits,
                self.gap,
                plan.layout.clone(),
            )?
        } else {
            SessionStore::new(
                backend,
                table,
                resources,
                self.input_schema.schema.clone(),
                self.limits,
                self.gap,
            )?
        });
        Ok(())
    }

    fn store(&self) -> Result<&SessionStore> {
        self.store
            .as_ref()
            .context("native SESSION was not started")
    }

    pub async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        converter: &Converter,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let watermark = ctx.last_present_watermark();
        if watermark.is_some_and(|wm| to_nanos(wm) == u64::MAX as u128) {
            return Ok(());
        }
        let timestamps = batch
            .column(self.input_schema.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .context("native SESSION timestamp type changed")?;
        let floor = watermark
            .map(|wm| i64::try_from(to_nanos(wm)))
            .transpose()?;
        let group_count = self.input_schema.routing_keys().map_or(0, Vec::len);
        for index in 0..batch.num_rows() {
            if timestamps.is_null(index) && floor.is_some() {
                continue;
            }
            ensure!(
                !timestamps.is_null(index),
                "native SESSION timestamp cannot be null"
            );
            if floor.is_some_and(|floor| timestamps.value(index) < floor) {
                continue;
            }
            // Eligible state consumes column views synchronously; do not copy
            // unrelated payload columns merely to feed COUNT/SUM state.
            let row = if self.incremental.is_some() {
                batch.slice(index, 1)
            } else {
                let indices = UInt32Array::from(vec![u32::try_from(index)?]);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None))
                    .collect::<arrow::error::Result<Vec<_>>>()?;
                RecordBatch::try_new(batch.schema(), columns)?
            };
            let group = if group_count == 0 {
                Vec::new()
            } else {
                converter
                    .convert_columns(&row.columns()[..group_count])?
                    .as_ref()
                    .to_vec()
            };
            if let Some(plan) = &self.incremental {
                self.store()?
                    .insert_incremental(&group, timestamps.value(index), || plan.updater(&row))
                    .await?;
            } else {
                self.store()?
                    .insert(&group, timestamps.value(index), &row)
                    .await?;
            }
        }
        Ok(())
    }

    async fn emit(
        &mut self,
        group: &[u8],
        session: SessionMeta,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
        converter: &Converter,
        state: Option<AdmittedSessionState>,
    ) -> Result<()> {
        let output_fields = ctx
            .out_schema
            .as_ref()
            .context("native SESSION output schema missing")?
            .schema
            .fields()
            .len();
        let output_bytes =
            output_allowance(self.finish.schema().as_ref(), output_fields, self.limits)?;
        // Retain admitted aggregate-state ownership through output acknowledgment.
        let aggregate_state = state;
        ensure!(
            self.incremental.is_some() == aggregate_state.is_some(),
            "SESSION output state mode changed"
        );
        let _queue = if self.incremental.is_none() {
            Some(self.store()?.reserve_final_input_queue()?)
        } else {
            None
        };
        let result = if let (Some(plan), Some(state)) = (&self.incremental, &aggregate_state) {
            plan.evaluate(&state.batch)?
        } else {
            let snapshot = self.store()?.snapshot().await?;
            let reader_schema = retained_input_schema(&self.finish)
                .context("native SESSION retained reader missing")?;
            let aggregate = self
                .finish
                .as_any()
                .downcast_ref::<AggregateExec>()
                .context("native SESSION aggregate missing")?;
            let descriptors = aggregate
                .aggr_expr()
                .iter()
                .map(|expression| -> Result<_> {
                    let Some(descriptor) = aggregate_descriptor(expression, &reader_schema)? else {
                        return Ok(None);
                    };
                    let column = if descriptor.payload_multiplier > 0 {
                        Some(
                            expression.expressions()[0]
                                .as_any()
                                .downcast_ref::<datafusion::physical_expr::expressions::Column>()
                                .context("native SESSION companion projection schema mismatch")?
                                .index(),
                        )
                    } else {
                        None
                    };
                    Ok(Some((descriptor, column)))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let mut retained = descriptors
                .iter()
                .try_fold(0usize, |sum, (descriptor, _)| {
                    sum.checked_add(descriptor.retained_bound(0, 0)?)
                })
                .context("native SESSION aggregate state size overflow")?;
            ensure!(
                retained <= self.limits.partial_bytes,
                "native SESSION retained aggregate state exceeds configured partial limit"
            );
            if descriptors.iter().any(|(descriptor, _)| {
                descriptor.state_list_column.is_some() || descriptor.payload_multiplier > 0
            }) {
                // Read already-projected arguments without evaluating SQL a
                // second time; account only the selected logical value bytes.
                let mut after = None;
                let mut rows = 0usize;
                let mut payloads = vec![0usize; descriptors.len()];
                while let Some(row) = self
                    .store()?
                    .next_row(&snapshot, group, session, after.as_deref())
                    .await?
                {
                    rows = rows
                        .checked_add(1)
                        .context("native SESSION retained row count overflow")?;
                    retained = 0;
                    for ((descriptor, column), payload) in descriptors.iter().zip(&mut payloads) {
                        if let Some(column) = column {
                            let bytes = logical_payload_bytes(row.batch.column(*column), 0)?;
                            *payload = if descriptor.payload_is_max {
                                (*payload).max(bytes)
                            } else {
                                payload
                                    .checked_add(bytes)
                                    .context("native SESSION retained payload overflow")?
                            };
                        }
                        retained = retained
                            .checked_add(
                                descriptor
                                    .retained_bound(rows, *payload)
                                    .context("native SESSION aggregate state size overflow")?,
                            )
                            .context("native SESSION companion total overflow")?;
                    }
                    ensure!(
                        retained <= self.limits.partial_bytes,
                        "native SESSION retained aggregate state exceeds configured partial limit"
                    );
                    after = Some(row.key);
                }
            }
            let _retained_permit = self.store()?.reserve_retained_aggregate(retained)?;
            // Establish headroom for the existing three-value retained reader
            // before constructing DF state. The reader owns its normal permit
            // while decoding; this probe creates no second lasting allowance.
            self.store()?.check_reader_capacity()?;
            let (sender, receiver) = channel(1);
            *self.finish_receiver.write().unwrap() = Some(receiver);
            self.finish.reset()?;
            let mut finish = self.finish.execute(0, self.task_context.clone())?;
            let store = self.store()?;
            let reader_schema = retained_input_schema(&self.finish)
                .context("native SESSION retained reader missing")?;
            let row_limit = self.limits.partial_bytes;
            let producer = async move {
                let mut after = None;
                while let Some(row) = store
                    .next_row(&snapshot, group, session, after.as_deref())
                    .await?
                {
                    after = Some(row.key);
                    let batch = if reader_schema.index_of(ORDINAL_FIELD).is_ok() {
                        let mut columns = row.batch.columns().to_vec();
                        columns.push(Arc::new(UInt64Array::from(vec![row.ordinal])));
                        let batch = RecordBatch::try_new(reader_schema.clone(), columns)?;
                        ensure!(
                            batch.get_array_memory_size() <= row_limit,
                            "native SESSION retained row with ordinal exceeds configured value limit"
                        );
                        batch
                    } else {
                        row.batch
                    };
                    sender.send(batch).await.map_err(|_| {
                        anyhow::anyhow!(
                            "native SESSION final aggregate stopped before consuming rows"
                        )
                    })?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let consumer = async {
                let mut result = None;
                while let Some(batch) = finish.next().await {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    ensure!(
                        result.is_none() && batch.num_rows() == 1,
                        "native SESSION final aggregate returned more than one row"
                    );
                    result = Some(batch);
                }
                Ok::<_, anyhow::Error>(result)
            };
            let (_, result) = try_join!(producer, consumer)?;
            result.context("native SESSION final aggregate returned no row")?
        };
        ensure!(
            result.get_array_memory_size() <= self.limits.partial_bytes,
            "native SESSION output aggregate exceeds configured value limit"
        );
        let key_columns = if let Some(parser) = converter.parser() {
            converter.convert_rows(vec![parser.parse(group)])?
        } else {
            vec![]
        };
        let start = PrimitiveArray::<TimestampNanosecondType>::from(vec![session.start]);
        let end = session
            .end
            .checked_add(self.gap)
            .context("native SESSION end overflow")?;
        let finish = PrimitiveArray::<TimestampNanosecondType>::from(vec![end]);
        let timestamp = PrimitiveArray::<TimestampNanosecondType>::from(vec![end - 1]);
        let DataType::Struct(fields) = self.window_field.data_type() else {
            anyhow::bail!("native SESSION window field is not a struct");
        };
        let window = StructArray::try_new(
            fields.clone(),
            vec![Arc::new(start), Arc::new(finish)],
            None,
        )?;
        let mut columns = key_columns;
        columns.insert(self.window_index, Arc::new(window));
        columns.extend_from_slice(result.columns());
        columns.push(Arc::new(timestamp));
        let output = RecordBatch::try_new(
            ctx.out_schema
                .as_ref()
                .context("native SESSION output schema missing")?
                .schema
                .clone(),
            columns,
        )?;
        ensure!(
            output.get_array_memory_size() <= output_bytes,
            "native SESSION output exceeds backend value allowance"
        );
        collector.collect(output).await?;
        Ok(())
    }

    pub async fn prepare_checkpoint(&self) -> Result<()> {
        // Usually handlers are sequential. If an embedding caller cancels a
        // watermark handler after output acknowledgment, capture its phase
        // before the table manager takes the checkpoint snapshot.
        if let Some((group, session)) = &self.acknowledged_closure {
            self.store()?.mark_closing(group, *session).await?;
        }
        Ok(())
    }

    pub async fn handle_watermark(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
        converter: &Converter,
    ) -> Result<()> {
        let Some(watermark) = ctx.last_present_watermark() else {
            return Ok(());
        };
        let floor = if to_nanos(watermark) == u64::MAX as u128 {
            None
        } else {
            Some(i64::try_from(to_nanos(watermark))?)
        };
        loop {
            let (due, closing) = if let Some((group, session)) = &self.acknowledged_closure {
                (Some((group.clone(), *session, None)), true)
            } else if self.incremental.is_some() {
                let due = self.store()?.first_due_incremental(floor).await?;
                let closing = due.as_ref().is_some_and(|(_, _, state)| state.is_none());
                (due, closing)
            } else {
                let due = self.store()?.first_due(floor).await?;
                let closing = match &due {
                    Some((group, session)) => self.store()?.is_closing(group, *session).await?,
                    None => false,
                };
                (due.map(|(group, session)| (group, session, None)), closing)
            };
            let Some((group, session, state)) = due else {
                break;
            };
            if self.acknowledged_closure.is_none() && !closing {
                self.emit(&group, session, ctx, collector, converter, state)
                    .await?;
                // Set synchronously after acknowledgment: cancellation while
                // persisting the phase cannot re-emit on this worker.
                self.acknowledged_closure = Some((group.clone(), session));
            }
            self.store()?.mark_closing(&group, session).await?;
            self.store()?.retire(&group, session).await?;
            self.acknowledged_closure = None;
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::Schema;

    #[tokio::test]
    #[ignore = "requires externally declared SQL, worker config and receipt paths"]
    async fn declared_incremental_session_route() {
        let path = |name| std::path::PathBuf::from(std::env::var(name).expect(name));
        let sql_path = path("STREAMR_DECLARED_SESSION_SQL");
        let config_path = path("STREAMR_DECLARED_SESSION_CONFIG");
        let receipt_path = path("STREAMR_DECLARED_SESSION_RECEIPT");
        let sql = std::fs::read(&sql_path).unwrap();
        let config_bytes = std::fs::read(&config_path).unwrap();
        arroyo_rpc::config::initialize_config(Some(&config_path), None);
        let limits = config()
            .worker
            .window_state
            .expect("declared window-state limits");
        let compiled = arroyo_planner::parse_and_get_program(
            std::str::from_utf8(&sql).unwrap(),
            arroyo_planner::ArroyoSchemaProvider::new(),
            arroyo_planner::SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        let registry = Arc::new(arroyo_planner::physical::new_registry());
        let mut sessions = Vec::new();
        for node in compiled.program.graph.node_weights() {
            for (operator, _) in node.operator_chain.iter() {
                if operator.operator_name
                    != arroyo_datastream::logical::OperatorName::SessionWindowAggregate
                {
                    continue;
                }
                let declared = api::SessionWindowAggregateOperator::decode(
                    operator.operator_config.as_slice(),
                )
                .unwrap();
                let native = NativeSession::new(&declared, registry.clone(), limits).unwrap();
                let incremental = native
                    .incremental
                    .as_ref()
                    .expect("declared SESSION must use incremental state");
                sessions.push(serde_json::json!({
                    "name": declared.name,
                    "operator_config_sha256": format!("{:x}", Sha256::digest(&operator.operator_config)),
                    "input_schema": native.input_schema.schema,
                    "routing_keys": native.input_schema.routing_keys(),
                    "state_schema": incremental.layout.schema,
                    "finish_schema": native.finish.schema(),
                    "max_encoded_bytes": incremental.layout.max_encoded_bytes,
                    "decoded_working_bytes": incremental.layout.decoded_working_bytes,
                    "layout_identity_hex": incremental.layout.identity.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                    "incremental": true
                }));
            }
        }
        assert!(
            !sessions.is_empty(),
            "declared SQL contains no SESSION operator"
        );
        let receipt = serde_json::json!({
            "sql_sha256": format!("{:x}", Sha256::digest(&sql)),
            "config_sha256": format!("{:x}", Sha256::digest(&config_bytes)),
            "sql_path": sql_path, "config_path": config_path,
            "scope": "actual planner and NativeSession constructor; no on_start or source processing",
            "sessions": sessions
        });
        std::fs::write(receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    }

    #[test]
    fn output_allowance_counts_each_variable_aggregate() {
        let schema = Schema::new(vec![
            Field::new("first_text", DataType::Utf8, true),
            Field::new("last_text", DataType::Utf8, true),
            Field::new("count", DataType::Int64, false),
        ]);
        let limits = WindowStateConfig {
            key_bytes: 128,
            partial_bytes: 1024,
            page_bytes: 8192,
            page_entries: 8,
            write_bytes: 32768,
            write_operations: 16,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        assert_eq!(
            output_allowance(&schema, 6, limits).unwrap(),
            2 * limits.partial_bytes + limits.key_bytes + 6 * 4096
        );
    }

    struct BlockedSessionCollector {
        started: Option<tokio::sync::oneshot::Sender<std::sync::Weak<dyn Array>>>,
        release: tokio::sync::oneshot::Receiver<()>,
    }

    fn assert_session_output(batch: &RecordBatch, start: i64, end: i64, sum: i64) {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
        let window = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let times = |column: &Arc<dyn Array>| {
            column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0)
        };
        assert_eq!(times(window.column(0)), start);
        assert_eq!(times(window.column(1)), end);
        let count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let total = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert!(!count.is_null(0));
        assert!(!total.is_null(0));
        assert_eq!(count.value(0), 2);
        assert_eq!(total.value(0), sum);
        let items = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let items = items.value(0);
        let items = items
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(
            items.values().as_ref(),
            if sum == 8 { &[3, 5] } else { &[7, 11] }
        );
        assert_eq!(times(batch.column(4)), end - 1);
    }

    #[async_trait::async_trait]
    impl Collector for BlockedSessionCollector {
        async fn collect(&mut self, batch: RecordBatch) -> arroyo_rpc::errors::DataflowResult<()> {
            assert_session_output(&batch, 0, 19, 8);
            self.started
                .take()
                .unwrap()
                .send(Arc::downgrade(batch.column(0)))
                .unwrap();
            (&mut self.release).await.unwrap();
            drop(batch);
            Ok(())
        }
        async fn broadcast_watermark(
            &mut self,
            _: arroyo_types::Watermark,
        ) -> arroyo_rpc::errors::DataflowResult<()> {
            panic!("native SESSION emits batches, not watermark signals");
        }
    }

    #[derive(Default)]
    struct RecordedSessionCollector(Vec<RecordBatch>);

    #[async_trait::async_trait]
    impl Collector for RecordedSessionCollector {
        async fn collect(&mut self, batch: RecordBatch) -> arroyo_rpc::errors::DataflowResult<()> {
            self.0.push(batch);
            Ok(())
        }
        async fn broadcast_watermark(
            &mut self,
            _: arroyo_types::Watermark,
        ) -> arroyo_rpc::errors::DataflowResult<()> {
            panic!("native SESSION emits batches, not watermark signals");
        }
    }

    async fn assert_retained_session(
        store: &SessionStore,
        session: SessionMeta,
        expected: &[(i64, i64)],
    ) {
        let snapshot = store.snapshot().await.unwrap();
        let mut after = None;
        let mut rows = Vec::new();
        while let Some(row) = store
            .next_row(&snapshot, &[], session, after.as_deref())
            .await
            .unwrap()
        {
            let value = row
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            let time = row
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            assert_eq!(row.batch.num_rows(), 1);
            assert!(!value.is_null(0));
            assert!(!time.is_null(0));
            rows.push((time.value(0), value.value(0)));
            after = Some(row.key.clone());
        }
        assert_eq!(rows, expected);
    }

    fn decoded_available(resources: &arroyo_state::live::resources::WorkerStateResources) -> usize {
        let (mut low, mut high) = (0, resources.config().decoded_value_bytes);
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if resources.try_decoded_value(middle).is_ok() {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        low
    }

    fn assert_snapshot_capacity(resources: &arroyo_state::live::resources::WorkerStateResources) {
        use futures::FutureExt;
        let first = resources
            .snapshot()
            .now_or_never()
            .expect("first snapshot leaked")
            .unwrap();
        let second = resources
            .snapshot()
            .now_or_never()
            .expect("second snapshot leaked")
            .unwrap();
        assert!(
            resources.snapshot().now_or_never().is_none(),
            "snapshot limit was bypassed"
        );
        drop((first, second));
    }

    #[tokio::test]
    async fn planned_ordered_typed_arrays_and_filtered_metadata_execute_with_bounded_input() {
        use arroyo_state::live::{
            LiveStateBackend, Ownership,
            memory::MemoryLiveState,
            resources::{ResourceConfig, WorkerStateResources},
            table::LiveTableManager,
        };
        use std::time::{Duration, UNIX_EPOCH};
        for rocks in [false, true] {
            for capacity_case in 0..3 {
                let query = "CREATE TABLE session_input (timestamp TIMESTAMP NOT NULL, ordinal BIGINT NOT NULL,
                metric BIGINT NOT NULL, attribute TEXT NOT NULL, selected BOOLEAN NOT NULL,
                WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute')
                WITH (connector = 'single_file', path = '/tmp/native-session-plan-input.jsonl',
                      format = 'json', type = 'source');
                SELECT SESSION(INTERVAL '1 second') AS window, COUNT(*) AS n,
                    ARRAY_AGG(metric ORDER BY timestamp, ordinal) AS items,
                    ARRAY_AGG(attribute ORDER BY timestamp, ordinal) AS attributes,
                    ARRAY_AGG(selected ORDER BY timestamp, ordinal) AS selected_items,
                    FIRST_VALUE(attribute ORDER BY ordinal) FILTER (WHERE attribute <> '') AS first_attribute,
                    LAST_VALUE(attribute ORDER BY ordinal) FILTER (WHERE attribute <> '') AS last_attribute
                FROM session_input GROUP BY window";
                async fn compile(query: &str) -> api::SessionWindowAggregateOperator {
                    let compiled = arroyo_planner::parse_and_get_program(
                        query,
                        arroyo_planner::ArroyoSchemaProvider::new(),
                        arroyo_planner::SqlConfig {
                            default_parallelism: 1,
                        },
                    )
                    .await
                    .unwrap();
                    let operator = compiled
                        .program
                        .graph
                        .node_weights()
                        .flat_map(|node| node.operator_chain.iter())
                        .map(|(operator, _)| operator)
                        .find(|operator| {
                            operator.operator_name
                                == arroyo_datastream::logical::OperatorName::SessionWindowAggregate
                        })
                        .unwrap();
                    api::SessionWindowAggregateOperator::decode(operator.operator_config.as_slice())
                        .unwrap()
                }
                let limits = WindowStateConfig {
                    key_bytes: 128,
                    partial_bytes: 8192,
                    page_bytes: 32768,
                    page_entries: 1,
                    write_bytes: 65536,
                    write_operations: 16,
                    max_resident_bytes: 8 * 1024 * 1024,
                };
                let registry = Arc::new(arroyo_planner::physical::new_registry());
                let config = compile(query).await;
                let mut operator = NativeSession::new(&config, registry.clone(), limits).unwrap();
                assert_eq!(
                    *operator
                        .finish
                        .as_any()
                        .downcast_ref::<AggregateExec>()
                        .unwrap()
                        .mode(),
                    datafusion::physical_plan::aggregates::AggregateMode::Single
                );
                let expanded = compile(&query.replace(
                    "ARRAY_AGG(attribute ORDER BY timestamp, ordinal)",
                    "ARRAY_AGG(REPEAT(attribute, 1000000000) ORDER BY timestamp, ordinal)",
                ))
                .await;
                let error = NativeSession::new(&expanded, registry, limits)
                    .err()
                    .expect("expanded collection expression was admitted");
                assert!(error.to_string().contains("direct"), "{error}");
                let companion_bytes = operator
                    .finish
                    .as_any()
                    .downcast_ref::<AggregateExec>()
                    .unwrap()
                    .aggr_expr()
                    .iter()
                    .map(|expression| {
                        aggregate_descriptor(expression, &operator.input_schema.schema)
                            .unwrap()
                            .map(|descriptor| descriptor.retained_bound(0, 0).unwrap())
                            .unwrap_or(0)
                    })
                    .sum::<usize>();
                let decoded_value_bytes = match capacity_case {
                    0 => 9 * limits.partial_bytes,
                    1 => 9 * limits.partial_bytes + companion_bytes,
                    _ => 1024 * 1024,
                };
                let state = WorkerStateResources::new(ResourceConfig {
                    block_cache_bytes: 1024 * 1024,
                    memtable_bytes: 1024 * 1024,
                    queued_write_bytes: 1024 * 1024,
                    decoded_value_bytes,
                    scan_page_bytes: 1024 * 1024,
                    max_blocking_operations: 2,
                    max_snapshots: 2,
                    max_open_databases: 1,
                    disk_reserve_bytes: 0,
                })
                .unwrap();
                let root = std::env::temp_dir().join(format!(
                    "str118-native-{}-{decoded_value_bytes}",
                    std::process::id()
                ));
                let backend: Arc<dyn LiveStateBackend> = if rocks {
                    arroyo_state::live::worker::construct_backend(
                        arroyo_state::live::worker::BackendConstruction::Rocksdb(
                            arroyo_state::live::lifecycle::RocksStateConfig {
                                root: root.clone(),
                                job_id: "str118-native".into(),
                                operator_id: "session".into(),
                                subtask: 0,
                                generation: 0,
                                attempt: 0,
                            },
                        ),
                        state.clone(),
                    )
                    .await
                    .unwrap()
                } else {
                    Arc::new(
                        MemoryLiveState::bounded(state.clone(), limits.max_resident_bytes).unwrap(),
                    )
                };
                let mut tables = LiveTableManager::new(
                    backend.clone(),
                    Ownership::PartitionLocal {
                        subtask: 0,
                        parallelism: 1,
                    },
                )
                .unwrap();
                let table = tables.register(TABLE).unwrap();
                let schema = operator.input_schema.schema.clone();
                let store = SessionStore::new(
                    backend.clone(),
                    table,
                    state.clone(),
                    schema.clone(),
                    limits,
                    operator.gap,
                )
                .unwrap();
                for (time, ordinal, metric, attribute, selected) in
                    [(9, 0, 3, "later", true), (0, 1, 5, "", false)]
                {
                    let columns: Vec<Arc<dyn Array>> = schema
                        .fields()
                        .iter()
                        .map(|field| match field.data_type() {
                            DataType::Timestamp(_, _) => {
                                Arc::new(TimestampNanosecondArray::from(vec![time]))
                                    as Arc<dyn Array>
                            }
                            DataType::Int64 => {
                                Arc::new(arrow_array::Int64Array::from(vec![if field.name()
                                    == "ordinal"
                                {
                                    ordinal
                                } else {
                                    metric
                                }])) as Arc<dyn Array>
                            }
                            DataType::Utf8 => {
                                Arc::new(StringArray::from(vec![attribute])) as Arc<dyn Array>
                            }
                            DataType::Boolean => {
                                Arc::new(arrow_array::BooleanArray::from(vec![selected]))
                                    as Arc<dyn Array>
                            }
                            other => panic!("unexpected planned input field {other:?}"),
                        })
                        .collect();
                    store
                        .insert(
                            &[],
                            time,
                            &RecordBatch::try_new(schema.clone(), columns).unwrap(),
                        )
                        .await
                        .unwrap();
                }
                operator.store = Some(store);
                let mut fields = operator.finish.schema().fields().to_vec();
                fields.insert(operator.window_index, operator.window_field.clone());
                fields.push(Arc::new(Field::new(
                    arroyo_rpc::TIMESTAMP_FIELD,
                    DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                    false,
                )));
                let output_schema = Arc::new(
                    ArroyoSchema::from_schema_unkeyed(Arc::new(Schema::new(fields))).unwrap(),
                );
                let (control_tx, _control_rx) = channel(16);
                let mut ctx = OperatorContext::new(
                    Arc::new(arroyo_types::get_test_task_info()),
                    None,
                    control_tx,
                    1,
                    vec![operator.input_schema.clone()],
                    Some(output_schema),
                    HashMap::new(),
                )
                .await;
                ctx.watermarks.set(
                    0,
                    arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(2)),
                );
                let mut collector = RecordedSessionCollector::default();
                if capacity_case == 0 {
                    let error = operator
                        .handle_watermark(
                            &mut ctx,
                            &mut collector,
                            &Converter::new(vec![]).unwrap(),
                        )
                        .await
                        .unwrap_err();
                    assert!(error.to_string().contains("decoded"), "{error:#}");
                    assert!(collector.0.is_empty());
                    assert!(
                        operator
                            .store()
                            .unwrap()
                            .first_due(None)
                            .await
                            .unwrap()
                            .is_some()
                    );
                    drop((operator, tables));
                    if rocks {
                        backend.close().await.unwrap();
                        std::fs::remove_dir_all(root).unwrap();
                    }
                    continue;
                }
                operator
                    .handle_watermark(&mut ctx, &mut collector, &Converter::new(vec![]).unwrap())
                    .await
                    .unwrap();
                assert_eq!(collector.0.len(), 1);
                let output = &collector.0[0];
                let window = output
                    .column(operator.window_index)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let time = |array: &Arc<dyn Array>| {
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(0)
                };
                assert_eq!(time(window.column(0)), 0);
                assert_eq!(time(window.column(1)), 1_000_000_009);
                assert_eq!(time(output.columns().last().unwrap()), 1_000_000_008);

                // SQL aliases are applied by the downstream projection; inspect
                // the physical aggregate columns in the query's declared order.
                let column = |index: usize| output.column(index).clone();
                let list = |index| {
                    column(index)
                        .as_any()
                        .downcast_ref::<arrow_array::ListArray>()
                        .unwrap()
                        .value(0)
                };
                assert_eq!(
                    column(1)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap()
                        .value(0),
                    2
                );
                assert_eq!(
                    list(2)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    &[5, 3]
                );
                let attributes = list(3);
                let attributes = attributes.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(
                    attributes.iter().collect::<Vec<_>>(),
                    vec![Some(""), Some("later")]
                );
                let selected = list(4);
                let selected = selected
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .unwrap();
                assert_eq!(
                    selected.iter().collect::<Vec<_>>(),
                    vec![Some(false), Some(true)]
                );
                for index in [5, 6] {
                    assert_eq!(
                        column(index)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "later"
                    );
                }
                assert!(
                    operator
                        .store()
                        .unwrap()
                        .first_due(None)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert_snapshot_capacity(&state);
                drop((operator, tables));
                if rocks {
                    backend.close().await.unwrap();
                    std::fs::remove_dir_all(root).unwrap();
                }
            }
        }
    }

    struct BlockedMedianCollector(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    );
    #[async_trait::async_trait]
    impl Collector for BlockedMedianCollector {
        async fn collect(&mut self, _: RecordBatch) -> arroyo_rpc::errors::DataflowResult<()> {
            let (dummy, _) = tokio::sync::oneshot::channel();
            let _ = std::mem::replace(&mut self.0, dummy).send(());
            let _ = (&mut self.1).await;
            Ok(())
        }
        async fn broadcast_watermark(
            &mut self,
            _: arroyo_types::Watermark,
        ) -> arroyo_rpc::errors::DataflowResult<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn median_session_shared_preflight_refusal_and_cancellation_preserve_rows() {
        use arroyo_planner::physical::ArroyoMemExec;
        use arroyo_state::live::{
            LiveStateBackend, Ownership,
            memory::MemoryLiveState,
            resources::{ResourceConfig, WorkerStateResources},
            table::LiveTableManager,
        };
        use datafusion::{
            functions_aggregate::median::median_udaf,
            physical_expr::{aggregate::AggregateExprBuilder, expressions::col},
            physical_plan::aggregates::{AggregateMode, PhysicalGroupBy},
        };
        use std::time::Duration;
        let limits = WindowStateConfig {
            key_bytes: 128,
            partial_bytes: 8192,
            page_bytes: 32768,
            page_entries: 1,
            write_bytes: 65536,
            write_operations: 16,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        let resources = WorkerStateResources::new(ResourceConfig {
            block_cache_bytes: 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            queued_write_bytes: 1024 * 1024,
            decoded_value_bytes: 1024 * 1024,
            scan_page_bytes: 1024 * 1024,
            max_blocking_operations: 2,
            max_snapshots: 8,
            max_open_databases: 1,
            disk_reserve_bytes: 0,
        })
        .unwrap();
        let backend: Arc<dyn LiveStateBackend> = Arc::new(
            MemoryLiveState::bounded(resources.clone(), limits.max_resident_bytes).unwrap(),
        );
        let mut tables = LiveTableManager::new(
            backend.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("metric", DataType::Int64, false),
            Field::new(
                arroyo_rpc::TIMESTAMP_FIELD,
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        let table = tables.register(TABLE).unwrap();
        let store = SessionStore::new(
            backend,
            table,
            resources.clone(),
            schema.clone(),
            limits,
            10,
        )
        .unwrap();
        for value in 0..9 {
            let row = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow_array::Int64Array::from(vec![value])),
                    Arc::new(TimestampNanosecondArray::from(vec![0])),
                ],
            )
            .unwrap();
            store.insert(&[], 0, &row).await.unwrap();
        }
        let aggregates = (0..2)
            .map(|index| {
                Arc::new(
                    AggregateExprBuilder::new(median_udaf(), vec![col("metric", &schema).unwrap()])
                        .schema(schema.clone())
                        .alias(format!("median_{index}"))
                        .build()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let planning: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![]),
                aggregates,
                vec![None; 2],
                Arc::new(ArroyoMemExec::new("input".into(), schema.clone())),
                schema.clone(),
            )
            .unwrap(),
        );
        let receiver = Arc::new(RwLock::new(None));
        let serialized = PhysicalPlanNode::try_from_physical_plan(
            planning,
            &ArroyoPhysicalExtensionCodec {
                context: DecodingContext::Planning,
            },
        )
        .unwrap();
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let finish = serialized
            .try_into_physical_plan(
                &arroyo_planner::physical::new_registry(),
                &runtime,
                &ArroyoPhysicalExtensionCodec {
                    context: DecodingContext::BoundedBatchStream(receiver.clone()),
                },
            )
            .unwrap();
        let window = Arc::new(Field::new("window", window_arrow_struct(), true));
        let output_schema = Arc::new(Schema::new(vec![
            window.as_ref().clone(),
            finish.schema().field(0).clone(),
            finish.schema().field(1).clone(),
            schema.field(1).clone(),
        ]));
        let input_schema = Arc::new(ArroyoSchema::from_schema_unkeyed(schema).unwrap());
        let mut operator = NativeSession {
            gap: 10,
            input_schema: input_schema.clone(),
            output_schema: Arc::new(
                ArroyoSchema::from_schema_unkeyed(output_schema.clone()).unwrap(),
            ),
            window_field: window,
            window_index: 0,
            finish,
            finish_receiver: receiver,
            limits,
            task_context: SessionContext::new_with_config_rt(Default::default(), runtime)
                .task_ctx(),
            acknowledged_closure: None,
            identity: vec![],
            store: Some(store),
            incremental: None,
        };
        let (control, _rx) = channel(16);
        let mut ctx = OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control,
            1,
            vec![input_schema],
            Some(Arc::new(
                ArroyoSchema::from_schema_unkeyed(output_schema).unwrap(),
            )),
            HashMap::new(),
        )
        .await;
        let converter = Converter::new(vec![]).unwrap();
        let session = SessionMeta { start: 0, end: 0 };
        // The historical queue(6 values)+reader(3 values) envelope leaves no
        // room for real companion ownership. Demonstrate the exact shared
        // capacity refusal without inflating the positive fixture's pool.
        let baseline = decoded_available(&resources);
        let tight = resources
            .try_decoded_value(baseline - 9 * limits.partial_bytes)
            .unwrap();
        let mut tight_output = RecordedSessionCollector::default();
        let resource_error = operator
            .emit(&[], session, &mut ctx, &mut tight_output, &converter, None)
            .await
            .unwrap_err();
        assert!(
            operator.finish_receiver.read().unwrap().is_none(),
            "DF reader bound before preflight failure: {resource_error:#}"
        );
        assert!(tight_output.0.is_empty());
        assert_retained_session(
            operator.store().unwrap(),
            session,
            &(0..9).map(|value| (0, value)).collect::<Vec<_>>(),
        )
        .await;
        drop(tight);
        assert!(
            decoded_available(&resources) >= baseline,
            "companion/input permits leaked"
        );
        operator.limits.partial_bytes = operator
            .finish
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap()
            .aggr_expr()
            .iter()
            .map(|expression| {
                aggregate_descriptor(expression, &operator.input_schema.schema)
                    .unwrap()
                    .unwrap()
                    .retained_bound(9, 0)
                    .unwrap()
            })
            .sum::<usize>()
            - 1;
        let mut collector = RecordedSessionCollector::default();
        let failure = operator
            .emit(&[], session, &mut ctx, &mut collector, &converter, None)
            .await
            .unwrap_err();
        assert!(failure.to_string().contains("retained aggregate state"));
        assert!(collector.0.is_empty());
        assert_retained_session(
            operator.store().unwrap(),
            session,
            &(0..9).map(|value| (0, value)).collect::<Vec<_>>(),
        )
        .await;
        assert!(
            decoded_available(&resources) >= baseline,
            "companion/input permits leaked"
        );
        operator.limits = limits;
        let (started, mut started_rx) = tokio::sync::oneshot::channel();
        let (_release, release_rx) = tokio::sync::oneshot::channel();
        let mut blocked = BlockedMedianCollector(started, release_rx);
        let mut pending =
            Box::pin(operator.emit(&[], session, &mut ctx, &mut blocked, &converter, None));
        tokio::time::timeout(Duration::from_secs(2),async {
            tokio::select! { result=&mut pending=>panic!("unexpected completion {result:?}"), ready=&mut started_rx=>ready.unwrap() }
        }).await.unwrap();
        drop(pending);
        assert_retained_session(
            operator.store().unwrap(),
            session,
            &(0..9).map(|value| (0, value)).collect::<Vec<_>>(),
        )
        .await;
        assert!(
            decoded_available(&resources) >= baseline,
            "companion/input permits leaked"
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            operator.emit(&[], session, &mut ctx, &mut collector, &converter, None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(collector.0.len(), 1);
        for column in &collector.0[0].columns()[1..3] {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                4
            );
        }
    }

    #[tokio::test]
    async fn watermark_cancellation_keeps_session_state_and_releases_backend_pages() {
        use arroyo_planner::physical::ArroyoMemExec;
        use arroyo_state::live::{
            LiveStateBackend, Ownership,
            memory::MemoryLiveState,
            resources::{ResourceConfig, WorkerStateResources},
            table::LiveTableManager,
        };
        use datafusion::functions_aggregate::{
            array_agg::array_agg_udaf, count::count_udaf, sum::sum_udaf,
        };
        use datafusion::physical_expr::{aggregate::AggregateExprBuilder, expressions::col};
        use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
        use std::time::{Duration, UNIX_EPOCH};

        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new("metric", DataType::Int64, false),
                Field::new(
                    arroyo_rpc::TIMESTAMP_FIELD,
                    DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                    false,
                ),
            ]));
            let aggregates = vec![
                Arc::new(
                    AggregateExprBuilder::new(count_udaf(), vec![col("metric", &schema).unwrap()])
                        .schema(schema.clone())
                        .alias("n")
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    AggregateExprBuilder::new(sum_udaf(), vec![col("metric", &schema).unwrap()])
                        .schema(schema.clone())
                        .alias("total")
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    AggregateExprBuilder::new(
                        array_agg_udaf(),
                        vec![col("metric", &schema).unwrap()],
                    )
                    .schema(schema.clone())
                    .alias("items")
                    .order_by(datafusion::physical_expr::LexOrdering::new(vec![
                        datafusion::physical_expr::PhysicalSortExpr::new(
                            col(arroyo_rpc::TIMESTAMP_FIELD, &schema).unwrap(),
                            arrow::compute::SortOptions::default(),
                        ),
                    ]))
                    .build()
                    .unwrap(),
                ),
            ];
            let companion_bytes = aggregates
                .iter()
                .map(|expression| {
                    aggregate_descriptor(expression, &schema)
                        .unwrap()
                        .map(|descriptor| descriptor.retained_bound(0, 0).unwrap())
                        .unwrap_or(0)
                })
                .sum::<usize>();
            let limits = WindowStateConfig {
                key_bytes: 128,
                partial_bytes: 1024.max(companion_bytes),
                page_bytes: 8192,
                page_entries: 8,
                write_bytes: 32768,
                write_operations: 16,
                max_resident_bytes: 8 * 1024 * 1024,
            };
            let state = WorkerStateResources::new(ResourceConfig {
                block_cache_bytes: 1024 * 1024,
                memtable_bytes: 1024 * 1024,
                queued_write_bytes: 1024 * 1024,
                decoded_value_bytes: limits.partial_bytes * 9 + companion_bytes,
                scan_page_bytes: 1024 * 1024,
                max_blocking_operations: 2,
                max_snapshots: 2,
                max_open_databases: 1,
                disk_reserve_bytes: 0,
            })
            .unwrap();
            let backend: Arc<dyn LiveStateBackend> = Arc::new(
                MemoryLiveState::bounded(state.clone(), limits.max_resident_bytes).unwrap(),
            );
            let mut tables = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )
            .unwrap();
            let table = tables.register(TABLE).unwrap();
            let mut store = SessionStore::new(
                backend.clone(),
                table.clone(),
                state.clone(),
                schema.clone(),
                limits,
                10,
            )
            .unwrap();
            for (time, value) in [(0, 3), (9, 5), (40, 7), (49, 11)] {
                let row = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![value])),
                        Arc::new(TimestampNanosecondArray::from(vec![time])),
                    ],
                )
                .unwrap();
                store.insert(&[], time, &row).await.unwrap();
            }
            let observed = super::super::session_store::tests::observe(&mut store);
            let observer =
                SessionStore::new(backend, table, state.clone(), schema.clone(), limits, 10)
                    .unwrap();
            // This separate read-only assertion owner must not consume the
            // closure's newly derived mandatory companion headroom.
            observer.clear_test_proof_caches();
            let input: Arc<dyn ExecutionPlan> =
                Arc::new(ArroyoMemExec::new("input".into(), schema.clone()));
            let planning: Arc<dyn ExecutionPlan> = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Single,
                    PhysicalGroupBy::new_single(vec![]),
                    aggregates,
                    vec![None, None, None],
                    input,
                    schema.clone(),
                )
                .unwrap(),
            );
            let receiver = Arc::new(RwLock::new(None));
            let codec = ArroyoPhysicalExtensionCodec {
                context: DecodingContext::Planning,
            };
            let serialized = PhysicalPlanNode::try_from_physical_plan(planning, &codec).unwrap();
            let codec = ArroyoPhysicalExtensionCodec {
                context: DecodingContext::BoundedBatchStream(receiver.clone()),
            };
            let finish = serialized
                .try_into_physical_plan(&arroyo_planner::physical::new_registry(), &runtime, &codec)
                .unwrap();
            validate_aggregates(finish.as_ref(), &schema).unwrap();
            output_allowance(finish.schema().as_ref(), 5, limits).unwrap();
            let window = Arc::new(Field::new("window", window_arrow_struct(), true));
            let output_schema = Arc::new(Schema::new(vec![
                window.as_ref().clone(),
                finish.schema().field(0).clone(),
                finish.schema().field(1).clone(),
                finish.schema().field(2).clone(),
                Field::new(
                    arroyo_rpc::TIMESTAMP_FIELD,
                    DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                    false,
                ),
            ]));
            let input_schema = Arc::new(ArroyoSchema::from_schema_unkeyed(schema).unwrap());
            let mut operator = NativeSession {
                gap: 10,
                input_schema: input_schema.clone(),
                output_schema: Arc::new(
                    ArroyoSchema::from_schema_unkeyed(output_schema.clone()).unwrap(),
                ),
                window_field: window,
                window_index: 0,
                finish,
                finish_receiver: receiver,
                limits,
                task_context: SessionContext::new_with_config_rt(
                    Default::default(),
                    runtime.clone(),
                )
                .task_ctx(),
                acknowledged_closure: None,
                identity: vec![],
                store: Some(store),
                incremental: None,
            };
            let (control_tx, _control_rx) = channel(16);
            let mut ctx = OperatorContext::new(
                Arc::new(arroyo_types::get_test_task_info()),
                None,
                control_tx,
                1,
                vec![input_schema],
                Some(Arc::new(
                    ArroyoSchema::from_schema_unkeyed(output_schema).unwrap(),
                )),
                HashMap::new(),
            )
            .await;
            ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_nanos(20)),
            );
            let converter = Converter::new(vec![]).unwrap();
            let due = SessionMeta { start: 0, end: 9 };
            let future = SessionMeta { start: 40, end: 49 };
            assert_eq!(
                observer.first_due(Some(20)).await.unwrap(),
                Some((vec![], due))
            );
            // The historical1KiB aggregate cap does not cover the actual
            // typed COUNT/SUM state wrappers. Retain that explicit refusal,
            // then use the source-derived cap and shared decoded headroom.
            operator.limits.partial_bytes = 1024;
            let mut refused = RecordedSessionCollector::default();
            let error = operator
                .handle_watermark(&mut ctx, &mut refused, &converter)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("retained aggregate state"),
                "{error:#}"
            );
            assert!(refused.0.is_empty());
            assert_retained_session(&observer, due, &[(0, 3), (9, 5)]).await;
            operator.limits = limits;
            let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let mut collector = BlockedSessionCollector {
                started: Some(started_tx),
                release: release_rx,
            };
            let mut pending =
                Box::pin(operator.handle_watermark(&mut ctx, &mut collector, &converter));
            let weak = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut pending => panic!("collector did not block: {result:?}"),
                    started = &mut started_rx => started.unwrap(),
                }
            })
            .await
            .unwrap();
            assert!(futures::poll!(&mut pending).is_pending());
            assert!(
                weak.upgrade().is_some(),
                "output ownership ended before collector acknowledgement"
            );
            // Producer/consumer have completed, releasing their snapshot. The
            // two-slot decoded input allowance stays held through collection.
            assert_snapshot_capacity(&state);
            let free = state.config().decoded_value_bytes - limits.partial_bytes * 6;
            assert!(state.try_decoded_value(free + 1).is_err());
            drop(state.try_decoded_value(free).unwrap());
            assert_retained_session(&observer, due, &[(0, 3), (9, 5)]).await;
            assert_retained_session(&observer, future, &[(40, 7), (49, 11)]).await;
            drop(pending);
            drop(collector);
            assert!(
                release_tx.send(()).is_err(),
                "cancelled collector receiver remained owned"
            );
            assert!(weak.upgrade().is_none(), "cancelled output remained owned");
            operator.store().unwrap().clear_test_proof_caches();
            observer.clear_test_proof_caches();
            drop(
                state
                    .try_decoded_value(state.config().decoded_value_bytes)
                    .unwrap(),
            );
            assert_snapshot_capacity(&state);
            assert_eq!(
                observer.first_due(Some(20)).await.unwrap(),
                Some((vec![], due))
            );
            assert_retained_session(&observer, due, &[(0, 3), (9, 5)]).await;
            assert_retained_session(&observer, future, &[(40, 7), (49, 11)]).await;
            assert!(operator.finish_receiver.read().unwrap().is_none());
            let mut emitted = RecordedSessionCollector::default();
            operator
                .handle_watermark(&mut ctx, &mut emitted, &converter)
                .await
                .unwrap();
            assert_eq!(emitted.0.len(), 1);
            assert_session_output(&emitted.0[0], 0, 19, 8);
            assert!(observer.first_due(Some(20)).await.unwrap().is_none());
            assert_retained_session(&observer, due, &[]).await;
            assert_retained_session(&observer, future, &[(40, 7), (49, 11)]).await;
            operator
                .handle_watermark(&mut ctx, &mut emitted, &converter)
                .await
                .unwrap();
            assert_eq!(
                emitted.0.len(),
                1,
                "retry emitted the retired session twice"
            );
            ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_nanos(60)),
            );
            observed.pause_writes(true);
            let mut pending =
                Box::pin(operator.handle_watermark(&mut ctx, &mut emitted, &converter));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    assert!(futures::poll!(&mut pending).is_pending());
                    if observed.paused_write_reached() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(pending);
            assert_eq!(
                emitted.0.len(),
                2,
                "collector acknowledgment did not precede phase write"
            );
            assert_eq!(operator.acknowledged_closure, Some((vec![], future)));
            assert_snapshot_capacity(&state);
            assert!(!observer.is_closing(&[], future).await.unwrap());
            assert_retained_session(&observer, future, &[(40, 7), (49, 11)]).await;
            observed.pause_writes(false);
            operator.prepare_checkpoint().await.unwrap();
            assert!(observer.is_closing(&[], future).await.unwrap());
            // A fresh operator has no RAM acknowledgment; the checkpointed
            // phase still skips emission and finishes bounded retirement.
            operator.acknowledged_closure = None;
            operator
                .handle_watermark(&mut ctx, &mut emitted, &converter)
                .await
                .unwrap();
            assert_eq!(emitted.0.len(), 2);
            assert_session_output(&emitted.0[1], 40, 59, 18);
            assert!(observer.first_due(None).await.unwrap().is_none());
            assert_retained_session(&observer, future, &[]).await;
            assert_snapshot_capacity(&state);
            let mut retained_input_bytes = 0usize;
            for time in 80..104 {
                let row = RecordBatch::try_new(
                    operator.input_schema.schema.clone(),
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![1])),
                        Arc::new(TimestampNanosecondArray::from(vec![time])),
                    ],
                )
                .unwrap();
                retained_input_bytes += row.get_array_memory_size();
                operator
                    .store()
                    .unwrap()
                    .insert(&[], time, &row)
                    .await
                    .unwrap();
            }
            assert!(retained_input_bytes > limits.partial_bytes);
            ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_nanos(114)),
            );
            operator
                .handle_watermark(&mut ctx, &mut emitted, &converter)
                .await
                .unwrap();
            assert_eq!(emitted.0.len(), 3);
            let fitted = &emitted.0[2];
            assert_eq!(fitted.num_rows(), 1);
            assert_eq!(fitted.num_columns(), 5);
            for column in [1, 2] {
                let value = fitted
                    .column(column)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                assert_eq!(value.iter().collect::<Vec<_>>(), vec![Some(24)]);
            }
            let window = fitted
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            for (index, expected) in [(0, 80), (1, 113)] {
                assert_eq!(
                    window
                        .column(index)
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(expected)]
                );
            }
            let items = fitted
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .unwrap();
            assert!(!items.is_null(0));
            let items = items.value(0);
            let items = items
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            assert_eq!(items.iter().collect::<Vec<_>>(), vec![Some(1); 24]);
            assert_eq!(
                fitted
                    .column(4)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(112)]
            );
            assert!(observer.first_due(None).await.unwrap().is_none());
            // Individual stored rows fit, but the actual 128-member final
            // collection exceeds the unchanged backend value limit.
            for time in 200..328 {
                let row = RecordBatch::try_new(
                    operator.input_schema.schema.clone(),
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![1])),
                        Arc::new(TimestampNanosecondArray::from(vec![time])),
                    ],
                )
                .unwrap();
                operator
                    .store()
                    .unwrap()
                    .insert(&[], time, &row)
                    .await
                    .unwrap();
            }
            ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_nanos(338)),
            );
            let error = operator
                .handle_watermark(&mut ctx, &mut emitted, &converter)
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("output aggregate exceeds configured value limit"),
                "{error:#}"
            );
            assert_eq!(emitted.0.len(), 3, "oversized final value was emitted");
            assert_snapshot_capacity(&state);
            let oversized = SessionMeta {
                start: 200,
                end: 327,
            };
            assert!(!observer.is_closing(&[], oversized).await.unwrap());
            assert_retained_session(
                &observer,
                oversized,
                &(200..328).map(|time| (time, 1)).collect::<Vec<_>>(),
            )
            .await;
        }
    }
    #[tokio::test]
    async fn incremental_sql_native_lifecycle_checkpoint_fresh_owner() {
        use std::time::{Duration, UNIX_EPOCH};
        const CHILD: &str = "STR138_NATIVE_LIFECYCLE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["arrow::session_native::tests::incremental_sql_native_lifecycle_checkpoint_fresh_owner", "--exact", "--nocapture"])
                .env(CHILD, "1").spawn().unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(90);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        assert!(status.success(), "native lifecycle child: {status}");
                        return;
                    }
                    Ok(None) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    result => {
                        let killed = child.kill();
                        let joined = child.wait();
                        panic!("native lifecycle child timeout: {result:?}/{killed:?}/{joined:?}");
                    }
                }
            }
        }
        use arroyo_state::live::{Ownership, StateNamespace, checkpoint};
        let root = std::env::temp_dir().join(format!("str138-native-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("worker.yaml");
        std::fs::write(&path, serde_json::to_vec(&serde_json::json!({
            "checkpoint-url": format!("file://{}", root.join("remote").display()),
            "worker": { "sql-state-backend": "memory", "disk-sql-state": { "directory": root.join("live"), "max-row-bytes": 8192 },
                "live-state-resources": { "block-cache-bytes": 1048576, "memtable-bytes": 524288, "queued-write-bytes": 8388608,
                    "decoded-value-bytes": 8388608, "scan-page-bytes": 2097152, "max-blocking-operations": 2,
                    "max-snapshots": 8, "max-open-databases": 4, "disk-reserve-bytes": 0 } }
        })).unwrap()).unwrap();
        arroyo_rpc::config::initialize_config(Some(&path), None);
        let query = "CREATE TABLE session_input (k TEXT, amount BIGINT, timestamp TIMESTAMP NOT NULL, WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute') WITH (connector='single_file',path='/tmp/native-session-incremental.jsonl',format='json',type='source'); SELECT k, SESSION(INTERVAL '10 seconds') AS window, COUNT(*) AS n, SUM(amount) AS total FROM session_input GROUP BY k, window";
        let compiled = arroyo_planner::parse_and_get_program(
            query,
            arroyo_planner::ArroyoSchemaProvider::new(),
            arroyo_planner::SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        let node = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|n| n.operator_chain.iter())
            .map(|(o, _)| o)
            .find(|o| {
                o.operator_name == arroyo_datastream::logical::OperatorName::SessionWindowAggregate
            })
            .unwrap();
        let config =
            api::SessionWindowAggregateOperator::decode(node.operator_config.as_slice()).unwrap();
        let limits = WindowStateConfig {
            key_bytes: 512,
            partial_bytes: 32768,
            page_bytes: 131072,
            page_entries: 1,
            write_bytes: 524288,
            write_operations: 64,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        async fn start(
            config: &api::SessionWindowAggregateOperator,
            limits: WindowStateConfig,
        ) -> (NativeSession, OperatorContext, Converter) {
            let mut operator = NativeSession::new(
                config,
                Arc::new(arroyo_planner::physical::new_registry()),
                limits,
            )
            .unwrap();
            assert!(
                operator.incremental.is_some(),
                "actual constructor must select eligible SQL state"
            );
            let input = operator.input_schema.clone();
            let count = input.routing_keys().map_or(0, Vec::len);
            let converter = Converter::new(
                input
                    .schema
                    .fields()
                    .iter()
                    .take(count)
                    .map(|f| arrow::row::SortField::new(f.data_type().clone()))
                    .collect(),
            )
            .unwrap();
            // Use the actual serialized public descriptor, including key
            // aliases and Arroyo key metadata, just as Engine construction does.
            let output = operator.output_schema.clone();
            let declared: ArroyoSchema = config.output_schema.clone().unwrap().try_into().unwrap();
            assert_eq!(output.as_ref(), &declared);
            let (control_tx, _control_rx) = channel(32);
            let mut ctx = OperatorContext::new(
                Arc::new(arroyo_types::get_test_task_info()),
                None,
                control_tx,
                1,
                vec![input],
                Some(output),
                operator.tables(),
            )
            .await;
            operator.on_start(&mut ctx).await.unwrap();
            (operator, ctx, converter)
        }
        fn input(operator: &NativeSession, time: i64, value: i64) -> RecordBatch {
            let columns = operator
                .input_schema
                .schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Int64 => datafusion::common::ScalarValue::Int64(Some(value))
                        .to_array_of_size(1)
                        .unwrap(),
                    DataType::Utf8 => datafusion::common::ScalarValue::Utf8(Some("key".into()))
                        .to_array_of_size(1)
                        .unwrap(),
                    DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz) => {
                        datafusion::common::ScalarValue::TimestampNanosecond(Some(time), tz.clone())
                            .to_array_of_size(1)
                            .unwrap()
                    }
                    t => panic!("unexpected native SQL input: {t:?}"),
                })
                .collect();
            RecordBatch::try_new(operator.input_schema.schema.clone(), columns).unwrap()
        }
        #[derive(Default)]
        struct AckCollector {
            rows: Vec<RecordBatch>,
        }
        #[async_trait::async_trait]
        impl Collector for AckCollector {
            async fn collect(
                &mut self,
                batch: RecordBatch,
            ) -> arroyo_rpc::errors::DataflowResult<()> {
                self.rows.push(batch);
                Ok(())
            }
            async fn broadcast_watermark(
                &mut self,
                _watermark: arroyo_types::Watermark,
            ) -> arroyo_rpc::errors::DataflowResult<()> {
                Ok(())
            }
        }
        const SECOND: i64 = 1_000_000_000;
        for rocks in [false, true] {
            arroyo_rpc::config::update(|c| {
                c.worker.sql_state_backend = if rocks {
                    arroyo_rpc::config::SqlStateBackend::Rocksdb
                } else {
                    arroyo_rpc::config::SqlStateBackend::Memory
                }
            });
            let (mut operator, mut ctx, converter) = start(&config, limits).await;
            for (time, value) in [(0, 2), (20 * SECOND, 3)] {
                operator
                    .process_batch(input(&operator, time, value), &mut ctx, &converter)
                    .await
                    .unwrap();
            }
            let namespace = StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: TABLE.as_bytes().to_vec(),
            };
            let disk_config =
                DiskKeyedTableConfig::decode(operator.tables()[TABLE].config.as_slice()).unwrap();
            let storage =
                arroyo_state::get_storage_provider(&arroyo_state::StorageProviderFor::Controller {
                    storage_url: Some(format!(
                        "file://{}",
                        root.join(if rocks {
                            "checkpoint-rocks"
                        } else {
                            "checkpoint-memory"
                        })
                        .display()
                    )),
                })
                .await
                .unwrap();
            let backend = operator.store().unwrap().testing_backend();
            let snapshot = backend.snapshot().await.unwrap();
            let receipt = checkpoint::export(
                &snapshot,
                &namespace,
                &disk_config,
                &storage,
                "checkpoint-1/session",
                1,
                0,
                0,
            )
            .await
            .unwrap();
            drop(snapshot);
            let (mut restored, mut restored_ctx, restored_converter) = start(&config, limits).await;
            let restored_backend = restored.store().unwrap().testing_backend();
            assert!(!Arc::ptr_eq(&backend, &restored_backend));
            checkpoint::restore(
                restored_backend.as_ref(),
                &namespace,
                &disk_config,
                &receipt,
                &storage,
            )
            .await
            .unwrap();
            restored
                .process_batch(
                    input(&restored, 10 * SECOND, 5),
                    &mut restored_ctx,
                    &restored_converter,
                )
                .await
                .unwrap();
            restored_ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(31)),
            );
            let observed =
                crate::arrow::session_store::tests::observe(restored.store.as_mut().unwrap());
            observed.pause_writes(true);
            let mut collector = AckCollector::default();
            let mut pending = Box::pin(restored.handle_watermark(
                &mut restored_ctx,
                &mut collector,
                &restored_converter,
            ));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    assert!(futures::poll!(&mut pending).is_pending());
                    if observed.paused_write_reached() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(pending);
            assert_eq!(collector.rows.len(), 1);
            assert!(restored.acknowledged_closure.is_some());
            let result = &collector.rows[0];
            let aggregate = restored
                .finish
                .as_any()
                .downcast_ref::<AggregateExec>()
                .unwrap();
            let aggregate_name = |name: &str| {
                let index = aggregate
                    .aggr_expr()
                    .iter()
                    .position(|expr| expr.fun().name().eq_ignore_ascii_case(name))
                    .unwrap();
                restored.finish.schema().field(index).name().clone()
            };
            let count_name = aggregate_name("count");
            let sum_name = aggregate_name("sum");
            eprintln!("STR138 native emitted schema: {:?}", result.schema());
            assert_eq!(
                result.schema(),
                restored_ctx.out_schema.as_ref().unwrap().schema
            );
            assert_eq!(
                result
                    .column_by_name(&count_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                3
            );
            assert_eq!(
                result
                    .column_by_name(&sum_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                10
            );
            observed.pause_writes(false);
            restored.prepare_checkpoint().await.unwrap();
            let snapshot = restored_backend.snapshot().await.unwrap();
            let receipt = checkpoint::export(
                &snapshot,
                &namespace,
                &disk_config,
                &storage,
                "checkpoint-2/session",
                2,
                0,
                0,
            )
            .await
            .unwrap();
            drop(snapshot);
            let (mut closing, mut closing_ctx, closing_converter) = start(&config, limits).await;
            let closing_backend = closing.store().unwrap().testing_backend();
            checkpoint::restore(
                closing_backend.as_ref(),
                &namespace,
                &disk_config,
                &receipt,
                &storage,
            )
            .await
            .unwrap();
            closing_ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(31)),
            );
            let mut output = AckCollector::default();
            closing
                .handle_watermark(&mut closing_ctx, &mut output, &closing_converter)
                .await
                .unwrap();
            assert!(
                output.rows.is_empty(),
                "acknowledged output re-emitted after fresh recovery"
            );
            closing
                .process_batch(
                    input(&closing, 100 * SECOND, 7),
                    &mut closing_ctx,
                    &closing_converter,
                )
                .await
                .unwrap();
            closing_ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(111)),
            );
            closing
                .handle_watermark(&mut closing_ctx, &mut output, &closing_converter)
                .await
                .unwrap();
            assert_eq!(output.rows.len(), 1);
            assert_eq!(
                output.rows[0].schema(),
                closing_ctx.out_schema.as_ref().unwrap().schema
            );
            assert_eq!(
                output.rows[0]
                    .column_by_name(&count_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                1
            );
            assert_eq!(
                output.rows[0]
                    .column_by_name(&sum_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                7
            );
            // Distinct backend/owner restoration, not OS/process loss.
            // Drop every table/owner handle before awaiting native DB cleanup
            // while this test's Tokio runtime is still alive.
            drop((operator, ctx, restored, restored_ctx, closing, closing_ctx));
            drop(observed);
            assert_eq!(Arc::strong_count(&backend), 1, "original backend handles");
            assert_eq!(
                Arc::strong_count(&restored_backend),
                1,
                "restored backend handles"
            );
            assert_eq!(
                Arc::strong_count(&closing_backend),
                1,
                "closing backend handles"
            );
            backend.close().await.unwrap();
            restored_backend.close().await.unwrap();
            closing_backend.close().await.unwrap();

            // Exercise the actual response-aware SESSION wrapper around the
            // same native constructor/backend lifecycle, including a fresh
            // checkpoint-restored owner and attempt-local response sequencing.
            use super::super::session_aggregating_window::SessionAggregatingWindowFunc;
            use arroyo_operator::operator::ArrowOperator;
            use arroyo_types::event_edge::{EventEdgeSpec, EventLayout};
            use arroyo_types::event_response::{
                EVENT_RESPONSE_VERSION, EventContext, EventResponse,
            };
            #[derive(Default)]
            struct ResponseCollector {
                packets: Vec<EventResponse>,
                rows: Vec<RecordBatch>,
            }
            #[async_trait::async_trait]
            impl Collector for ResponseCollector {
                async fn collect(
                    &mut self,
                    batch: RecordBatch,
                ) -> arroyo_rpc::errors::DataflowResult<()> {
                    self.rows.push(batch);
                    Ok(())
                }
                async fn collect_response(
                    &mut self,
                    packet: EventResponse,
                ) -> arroyo_rpc::errors::DataflowResult<()> {
                    self.packets.push(packet);
                    Ok(())
                }
                async fn broadcast_watermark(
                    &mut self,
                    _: arroyo_types::Watermark,
                ) -> arroyo_rpc::errors::DataflowResult<()> {
                    Ok(())
                }
            }
            let packet = |sequence, nonce, time, rows| EventResponse {
                context: EventContext {
                    version: EVENT_RESPONSE_VERSION,
                    root_operator: 7,
                    root_subtask: 0,
                    attempt_nonce: [nonce; 16],
                    sequence,
                    trigger_nanos: time,
                },
                rows,
                touched_keys: None,
                native_body: None,
            };
            let (native, mut response_ctx, converter) = start(&config, limits).await;
            let first = input(&native, 0, 2);
            let next = input(&native, SECOND, 3);
            let response_backend = native.store().unwrap().testing_backend();
            let mut response_owner =
                SessionAggregatingWindowFunc::response_test_owner(native, &config, converter);
            let raw = Arc::new(EventEdgeSpec {
                root_operator: 7,
                root_subtask: 0,
                schema: response_ctx.in_schemas[0].schema.clone(),
                layout: EventLayout::Raw,
                maintenance: false,
            });
            let edge = response_owner
                .bind_event_response(std::slice::from_ref(&raw), &response_ctx)
                .unwrap()
                .unwrap();
            assert!(edge.maintenance);
            let mut captured = ResponseCollector::default();
            // A filtered-away source row still completes the original event.
            response_owner
                .process_response_index(
                    0,
                    1,
                    packet(1, 4, 0, None),
                    &mut response_ctx,
                    &mut captured,
                )
                .await
                .unwrap();
            response_owner
                .process_response_index(
                    0,
                    1,
                    packet(2, 4, 0, Some(first)),
                    &mut response_ctx,
                    &mut captured,
                )
                .await
                .unwrap();
            for sequence in [2, 4] {
                assert!(
                    response_owner
                        .process_response_index(
                            0,
                            1,
                            packet(sequence, 4, 0, None),
                            &mut response_ctx,
                            &mut captured
                        )
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("skips or repeats")
                );
            }
            assert_eq!(captured.packets.len(), 2);
            assert!(
                captured.rows.is_empty(),
                "SESSION must not finalize on original-event ACK"
            );
            response_owner
                .handle_checkpoint(
                    arroyo_types::CheckpointBarrier {
                        epoch: 1,
                        min_epoch: 0,
                        timestamp: UNIX_EPOCH,
                        then_stop: false,
                    },
                    &mut response_ctx,
                    &mut captured,
                )
                .await
                .unwrap();
            let snapshot = response_backend.snapshot().await.unwrap();
            let response_receipt = checkpoint::export(
                &snapshot,
                &namespace,
                &disk_config,
                &storage,
                "checkpoint-response/session",
                3,
                0,
                0,
            )
            .await
            .unwrap();
            drop(snapshot);
            let (restored_native, mut fresh_ctx, fresh_converter) = start(&config, limits).await;
            let fresh_backend = restored_native.store().unwrap().testing_backend();
            checkpoint::restore(
                fresh_backend.as_ref(),
                &namespace,
                &disk_config,
                &response_receipt,
                &storage,
            )
            .await
            .unwrap();
            let mut fresh = SessionAggregatingWindowFunc::response_test_owner(
                restored_native,
                &config,
                fresh_converter,
            );
            fresh.bind_event_response(&[raw], &fresh_ctx).unwrap();
            // Fresh attempt starts at one with a fresh nonce, while retained
            // SESSION state still contains the pre-checkpoint event.
            fresh
                .process_response_index(
                    0,
                    1,
                    packet(1, 8, SECOND, Some(next)),
                    &mut fresh_ctx,
                    &mut captured,
                )
                .await
                .unwrap();
            assert_eq!(captured.packets.len(), 3);
            assert!(
                captured
                    .packets
                    .iter()
                    .all(|p| p.rows.is_none() && p.touched_keys.is_none())
            );
            assert_eq!(captured.packets[2].context.attempt_nonce, [8; 16]);
            assert!(captured.rows.is_empty());
            fresh_ctx.watermarks.set(
                0,
                arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(12)),
            );
            fresh
                .handle_watermark(
                    arroyo_types::Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(12)),
                    &mut fresh_ctx,
                    &mut captured,
                )
                .await
                .unwrap();
            assert_eq!(captured.rows.len(), 1);
            let closed = &captured.rows[0];
            assert_eq!(
                closed
                    .column_by_name(&count_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                2
            );
            assert_eq!(
                closed
                    .column_by_name(&sum_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                5
            );
            assert_eq!(
                closed
                    .column(fresh_ctx.out_schema.as_ref().unwrap().timestamp_index)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0),
                11 * SECOND - 1
            );
            assert_eq!(
                captured.packets.len(),
                3,
                "watermark closure must not fabricate an event response"
            );
            drop((response_owner, response_ctx, fresh, fresh_ctx));
            response_backend.close().await.unwrap();
            fresh_backend.close().await.unwrap();
        }
    }
    #[tokio::test]
    async fn session_finalized_serialized_descriptor_and_context_are_required() {
        let sql = "CREATE TABLE events(k TEXT NOT NULL,event_time TIMESTAMP NOT NULL,
          WATERMARK FOR event_time AS event_time-INTERVAL '5 seconds')
          WITH(connector='single_file',path='/tmp/session-descriptor.jsonl',format='json',type='source');
          SELECT k,SESSION(INTERVAL '30 minutes') AS window,COUNT(*) AS n FROM events GROUP BY k,window";
        let compiled = arroyo_planner::parse_and_get_program(
            sql,
            arroyo_planner::ArroyoSchemaProvider::new(),
            arroyo_planner::SqlConfig {
                default_parallelism: 1,
            },
        )
        .await
        .unwrap();
        let operator = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .map(|(operator, _)| operator)
            .find(|operator| {
                operator.operator_name
                    == arroyo_datastream::logical::OperatorName::SessionWindowAggregate
            })
            .unwrap();
        let valid =
            api::SessionWindowAggregateOperator::decode(operator.operator_config.as_slice())
                .unwrap();
        let limits = WindowStateConfig {
            key_bytes: 512,
            partial_bytes: 32768,
            page_bytes: 131072,
            page_entries: 1,
            write_bytes: 524288,
            write_operations: 64,
            max_resident_bytes: 8 * 1024 * 1024,
        };
        let registry = Arc::new(arroyo_planner::physical::new_registry());
        let mut owner = NativeSession::new(&valid, registry.clone(), limits).unwrap();
        assert_eq!(owner.output_schema.schema.field(0).name(), "k");
        for corruption in ["missing", "clock", "window", "key"] {
            let mut invalid = valid.clone();
            match corruption {
                "missing" => invalid.output_schema = None,
                "clock" => invalid.output_schema.as_mut().unwrap().timestamp_index = u32::MAX,
                "window" => invalid.window_index = u64::MAX,
                "key" => {
                    let old: ArroyoSchema =
                        invalid.output_schema.take().unwrap().try_into().unwrap();
                    let mut fields = old.schema.fields().to_vec();
                    fields[0] = Arc::new(Field::new("k", DataType::Int64, false));
                    invalid.output_schema = Some(
                        ArroyoSchema::new(
                            Arc::new(Schema::new(fields)),
                            old.timestamp_index,
                            old.storage_keys().cloned(),
                            old.routing_keys().cloned(),
                        )
                        .into(),
                    );
                }
                _ => unreachable!(),
            }
            let invalid =
                api::SessionWindowAggregateOperator::decode(invalid.encode_to_vec().as_slice())
                    .unwrap();
            assert!(
                NativeSession::new(&invalid, registry.clone(), limits).is_err(),
                "{corruption}"
            );
            use arroyo_operator::operator::OperatorConstructor;
            assert!(
                super::super::session_aggregating_window::SessionAggregatingWindowConstructor
                    .with_config(invalid, registry.clone())
                    .is_err(),
                "outer {corruption}"
            );
        }
        let (control, _rx) = tokio::sync::mpsc::channel(16);
        let mut context = OperatorContext::new(
            Arc::new(arroyo_types::get_test_task_info()),
            None,
            control,
            1,
            vec![owner.input_schema.clone()],
            None,
            HashMap::new(),
        )
        .await;
        let error = owner.on_start(&mut context).await.unwrap_err();
        assert!(
            error.to_string().contains("context schemas differ"),
            "{error}"
        );
        assert!(
            owner.store.is_none(),
            "forged context must fail before backend construction"
        );
    }
}
