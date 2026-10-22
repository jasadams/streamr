//! The streaming UNION lowers each branch separately, without UnionExec's
//! schema merging. Preserve that declared contract at the native SELECT edge.
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::SchemaRef;
use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD, updating_meta_field};
use datafusion::common::{Result, Statistics, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;
use std::{any::Any, fmt, sync::Arc};

#[derive(Debug)]
pub struct UnionSchemaExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metadata: Option<usize>,
    properties: PlanProperties,
}
impl UnionSchemaExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Self> {
        let actual = input.schema();
        if actual.fields().len() != schema.fields().len() || actual.metadata() != schema.metadata()
        {
            return plan_err!("native UNION schema width/metadata mismatch");
        }
        let mut metadata = None;
        for (index, (from, to)) in actual.fields().iter().zip(schema.fields()).enumerate() {
            if to.name() == UPDATING_META_FIELD {
                if metadata.replace(index).is_some()
                    || to != &updating_meta_field()
                    || &Arc::new(from.as_ref().clone().with_nullable(false)) != to
                {
                    return plan_err!("native UNION requires exact canonical updating metadata");
                }
            } else if from != to {
                // SQL UNION widens only outer user-field nullability. Names,
                // nested types/fields, field metadata and the hidden clock stay exact.
                if from.name() == TIMESTAMP_FIELD
                    || from.is_nullable()
                    || !to.is_nullable()
                    || from.as_ref().clone().with_nullable(true) != **to
                {
                    return plan_err!(
                        "native UNION schema differs beyond user nullability widening"
                    );
                }
            }
        }
        let properties = input
            .properties()
            .clone()
            .with_eq_properties(EquivalenceProperties::new(schema.clone()));
        Ok(Self {
            input,
            schema,
            metadata,
            properties,
        })
    }
    fn normalize(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.schema() != self.input.schema() {
            return exec_err!("native UNION physical batch schema changed");
        }
        if let Some(index) = self.metadata {
            let metadata = batch
                .column(index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!("native UNION metadata is not Struct")
                })?;
            if metadata.null_count() != 0
                || metadata
                    .columns()
                    .iter()
                    .any(|child| child.null_count() != 0)
            {
                return exec_err!("native UNION metadata parent/flag/id must be nonnull");
            }
        }
        // Immutable ArrayRefs, same row order/count and allocations. This is
        // not a value calculation, cast, NULL replacement, or new admission.
        Ok(RecordBatch::try_new(
            self.schema.clone(),
            batch.columns().to_vec(),
        )?)
    }
}
impl DisplayAs for UnionSchemaExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnionSchemaExec")
    }
}
impl ExecutionPlan for UnionSchemaExec {
    fn name(&self) -> &str {
        "UnionSchemaExec"
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
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("native UNION schema requires one child");
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.schema.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let adapter = Self::try_new(self.input.clone(), self.schema.clone())?;
        let output = input.map(move |batch| adapter.normalize(batch?));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            output,
        )))
    }
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{
        ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext, new_registry,
    };
    use arrow::buffer::NullBuffer;
    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;

    fn fixture(null_role: Option<usize>) -> (RecordBatch, SchemaRef) {
        let ids = [[7u8; 16], [8u8; 16]];
        let id_array = if null_role == Some(2) {
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [None, Some(ids[1].as_slice())].into_iter(),
                16,
            )
            .unwrap()
        } else {
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice())).unwrap()
        };
        let flag: ArrayRef = Arc::new(BooleanArray::from(vec![
            if null_role == Some(1) {
                None
            } else {
                Some(false)
            },
            Some(false),
        ]));
        let metadata: ArrayRef = Arc::new(StructArray::new(
            arroyo_rpc::updating_meta_fields(),
            vec![flag, Arc::new(id_array)],
            (null_role == Some(0)).then(|| NullBuffer::from(vec![false, true])),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("optional", DataType::Int64, true),
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            updating_meta_field().as_ref().clone().with_nullable(true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![None, Some(3)])),
                Arc::new(TimestampNanosecondArray::from(vec![5, 6])),
                metadata,
            ],
        )
        .unwrap();
        let mut fields = schema.fields().to_vec();
        fields[0] = Arc::new(fields[0].as_ref().clone().with_nullable(true));
        fields[3] = updating_meta_field();
        (batch, Arc::new(Schema::new(fields)))
    }
    fn input(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(ArroyoMemExec::new("union_input".into(), schema))
    }

    #[test]
    fn union_schema_widens_user_field_and_validates_metadata_without_copying_values() {
        let (batch, declared) = fixture(None);
        let adapter = UnionSchemaExec::try_new(input(batch.schema()), declared.clone()).unwrap();
        let output = adapter.normalize(batch.clone()).unwrap();
        assert_eq!(output.schema(), declared);
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.column(1).null_count(), 1);
        for (original, rebound) in batch.columns().iter().zip(output.columns()) {
            assert!(Arc::ptr_eq(original, rebound));
        }
    }

    #[test]
    fn union_schema_rejects_noncanonical_fields_and_actual_null_metadata_roles() {
        let (batch, declared) = fixture(None);
        let original = declared.fields().to_vec();
        for (index, field) in
            [
                (1, original[1].as_ref().clone().with_nullable(false)),
                (0, Field::new("renamed", DataType::Int64, true)),
                (0, Field::new("v", DataType::UInt64, true)),
                (
                    0,
                    original[0].as_ref().clone().with_metadata(
                        [("private".into(), "changed".into())].into_iter().collect(),
                    ),
                ),
                (2, original[2].as_ref().clone().with_nullable(true)),
                (
                    3,
                    updating_meta_field().as_ref().clone().with_nullable(true),
                ),
                (
                    3,
                    updating_meta_field().as_ref().clone().with_metadata(
                        [("private".into(), "changed".into())].into_iter().collect(),
                    ),
                ),
                (
                    3,
                    Field::new(
                        UPDATING_META_FIELD,
                        DataType::Struct(
                            vec![
                                Field::new("is_retract", DataType::Boolean, false),
                                Field::new("id", DataType::FixedSizeBinary(16), true),
                            ]
                            .into(),
                        ),
                        false,
                    ),
                ),
            ]
        {
            let mut fields = original.clone();
            fields[index] = Arc::new(field);
            assert!(
                UnionSchemaExec::try_new(input(batch.schema()), Arc::new(Schema::new(fields)))
                    .is_err()
            );
        }
        let changed_schema_metadata = Arc::new(Schema::new_with_metadata(
            original,
            [("private".into(), "changed".into())].into_iter().collect(),
        ));
        assert!(UnionSchemaExec::try_new(input(batch.schema()), changed_schema_metadata).is_err());
        for role in 0..3 {
            let (batch, declared) = fixture(Some(role));
            let adapter = UnionSchemaExec::try_new(input(batch.schema()), declared).unwrap();
            let error = adapter.normalize(batch).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("metadata parent/flag/id must be nonnull"),
                "{error}"
            );
        }
    }

    #[test]
    fn union_schema_codec_roundtrip_preserves_exact_schema_and_one_child_contract() {
        let (batch, declared) = fixture(None);
        let input = input(batch.schema());
        let adapter: Arc<dyn ExecutionPlan> =
            Arc::new(UnionSchemaExec::try_new(input.clone(), declared.clone()).unwrap());
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        let mut encoded = Vec::new();
        codec.try_encode(adapter.clone(), &mut encoded).unwrap();
        let decoded = codec
            .try_decode(&encoded, &[input], &new_registry())
            .unwrap();
        assert_eq!(decoded.schema(), declared);
        assert_eq!(decoded.children().len(), 1);
        let actual = decoded.as_any().downcast_ref::<UnionSchemaExec>().unwrap();
        assert_eq!(
            actual.normalize(batch.clone()).unwrap(),
            RecordBatch::try_new(declared, batch.columns().to_vec()).unwrap()
        );
        assert!(codec.try_decode(&encoded, &[], &new_registry()).is_err());
        assert!(adapter.with_new_children(vec![]).is_err());
    }
}
