use crate::grpc::api;
use crate::{Converter, TIMESTAMP_FIELD};
use anyhow::Result;
use arrow::compute::kernels::numeric::div;
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use arrow::row::SortField;
use arrow_array::builder::{ArrayBuilder, make_builder};
use arrow_array::types::UInt64Type;
use arrow_array::{Array, PrimitiveArray, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arrow_ord::cmp::gt_eq;
use arrow_ord::partition::partition;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{ArrowError, FieldRef};
use arroyo_types::to_nanos;
use datafusion::common::{DataFusionError, Result as DFResult};
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;

pub fn window_arrow_struct() -> DataType {
    DataType::Struct(
        vec![
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
        .into(),
    )
}

/// Native source admission removes only the configured operation marker. Key
/// metadata is reset at this unkeyed boundary; Arrow field/schema metadata and
/// the source clock retain their exact native layout.
pub fn source_admission_output_schema(
    input: &ArroyoSchema,
    marker: Option<usize>,
    preserve_marker: bool,
) -> Result<ArroyoSchema> {
    anyhow::ensure!(
        input.timestamp_index < input.schema.fields().len(),
        "source FOR clock index is outside the input schema"
    );
    if let Some(index) = marker {
        let field = input.schema.fields().get(index).ok_or_else(|| {
            anyhow::anyhow!("source envelope marker index is outside the input schema")
        })?;
        anyhow::ensure!(
            index != input.timestamp_index
                && field.name() == crate::SOURCE_ENVELOPE_FIELD
                && field.data_type() == &DataType::UInt64
                && !field.is_nullable(),
            "invalid source envelope marker schema"
        );
    }
    anyhow::ensure!(
        !preserve_marker || marker.is_some(),
        "preserved source envelopes require a source marker"
    );
    let removed = marker.filter(|_| !preserve_marker);
    let schema = if let Some(index) = removed {
        Arc::new(
            input.schema.project(
                &(0..input.schema.fields().len())
                    .filter(|column| *column != index)
                    .collect::<Vec<_>>(),
            )?,
        )
    } else {
        input.schema.clone()
    };
    Ok(ArroyoSchema::new_unkeyed(
        schema,
        input.timestamp_index
            - usize::from(removed.is_some_and(|index| index < input.timestamp_index)),
    ))
}

/// Exact native finalized SESSION layout; shared by serialization and construction.
pub fn session_output_schema(
    input: &ArroyoSchema,
    aggregate: &Schema,
    window_name: &str,
    window_index: usize,
    declared: &ArroyoSchema,
) -> Result<ArroyoSchema> {
    anyhow::ensure!(
        input.timestamp_index < input.schema.fields().len(),
        "SESSION input clock index is outside schema"
    );
    anyhow::ensure!(
        input.schema.field(input.timestamp_index).name() == TIMESTAMP_FIELD
            && input.schema.field(input.timestamp_index).data_type()
                == &DataType::Timestamp(TimeUnit::Nanosecond, None),
        "SESSION input clock must be canonical nanoseconds"
    );
    anyhow::ensure!(
        input.schema.index_of(crate::UPDATING_META_FIELD).is_err(),
        "SESSION requires append input"
    );
    let keys = input.routing_keys().cloned().unwrap_or_default();
    anyhow::ensure!(
        keys.iter().copied().eq(0..keys.len()) && keys.len() <= input.schema.fields().len(),
        "SESSION grouping keys must occupy the native input prefix"
    );
    anyhow::ensure!(
        window_index <= keys.len() && !window_name.is_empty() && window_name != TIMESTAMP_FIELD,
        "SESSION window insertion index/name is invalid"
    );
    let mut fields = Vec::new();
    for (position, index) in keys.iter().enumerate() {
        let output_index = position + usize::from(position >= window_index);
        let output = declared
            .schema
            .fields()
            .get(output_index)
            .ok_or_else(|| anyhow::anyhow!("SESSION public grouping field missing"))?;
        // KeyCalculation owns internal __key_i names. The public producer
        // descriptor owns aliases; every other field attribute remains exact.
        let expected = input.schema.fields()[*index]
            .as_ref()
            .clone()
            .with_name(output.name());
        anyhow::ensure!(
            expected == **output,
            "SESSION public grouping field differs from native key layout"
        );
        fields.push(output.clone());
    }
    fields.insert(
        window_index,
        Arc::new(Field::new(window_name, window_arrow_struct(), true)),
    );
    fields.extend(aggregate.fields().iter().cloned());
    fields.push(Arc::new(Field::new(
        TIMESTAMP_FIELD,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )));
    let mut names = std::collections::HashSet::new();
    anyhow::ensure!(
        fields
            .iter()
            .all(|field| names.insert(field.name().clone())),
        "SESSION output repeats a field name"
    );
    Ok(ArroyoSchema::from_schema_keys(
        Arc::new(Schema::new_with_metadata(
            fields,
            aggregate.metadata().clone(),
        )),
        vec![],
    )?)
}

pub type ArroyoSchemaRef = Arc<ArroyoSchema>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ArroyoSchema {
    pub schema: Arc<Schema>,
    pub timestamp_index: usize,
    key_indices: Option<Vec<usize>>,
    /// If defined, these indices are used for routing (i.e., which subtask gets which piece of data)
    routing_key_indices: Option<Vec<usize>>,
}

impl TryFrom<api::ArroyoSchema> for ArroyoSchema {
    type Error = DataFusionError;
    fn try_from(schema_proto: api::ArroyoSchema) -> Result<Self, DataFusionError> {
        let schema: Schema = serde_json::from_str(&schema_proto.arrow_schema)
            .map_err(|e| DataFusionError::Plan(format!("Invalid arrow schema: {e}")))?;
        let timestamp_index = schema_proto.timestamp_index as usize;

        let key_indices = schema_proto.has_keys.then(|| {
            schema_proto
                .key_indices
                .into_iter()
                .map(|index| index as usize)
                .collect()
        });

        let routing_key_indices = schema_proto.has_routing_keys.then(|| {
            schema_proto
                .routing_key_indices
                .into_iter()
                .map(|index| index as usize)
                .collect()
        });

        Ok(Self {
            schema: Arc::new(schema),
            timestamp_index,
            key_indices,
            routing_key_indices,
        })
    }
}

impl From<ArroyoSchema> for api::ArroyoSchema {
    fn from(schema: ArroyoSchema) -> Self {
        let arrow_schema = serde_json::to_string(schema.schema.as_ref()).unwrap();
        let timestamp_index = schema.timestamp_index as u32;

        let has_keys = schema.key_indices.is_some();
        let key_indices = schema
            .key_indices
            .map(|ks| ks.into_iter().map(|index| index as u32).collect())
            .unwrap_or_default();

        let has_routing_keys = schema.routing_key_indices.is_some();
        let routing_key_indices = schema
            .routing_key_indices
            .map(|ks| ks.into_iter().map(|index| index as u32).collect())
            .unwrap_or_default();

        Self {
            arrow_schema,
            timestamp_index,
            key_indices,
            has_keys,
            routing_key_indices,
            has_routing_keys,
        }
    }
}

impl ArroyoSchema {
    pub fn new(
        schema: Arc<Schema>,
        timestamp_index: usize,
        key_indices: Option<Vec<usize>>,
        routing_key_indices: Option<Vec<usize>>,
    ) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices,
            routing_key_indices,
        }
    }
    pub fn new_unkeyed(schema: Arc<Schema>, timestamp_index: usize) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: None,
            routing_key_indices: None,
        }
    }
    pub fn new_keyed(schema: Arc<Schema>, timestamp_index: usize, key_indices: Vec<usize>) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
            routing_key_indices: None,
        }
    }

    pub fn from_fields(mut fields: Vec<Field>) -> Self {
        if !fields.iter().any(|f| f.name() == TIMESTAMP_FIELD) {
            fields.push(Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ));
        }

        Self::from_schema_keys(Arc::new(Schema::new(fields)), vec![]).unwrap()
    }

    pub fn from_schema_unkeyed(schema: Arc<Schema>) -> DFResult<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no {TIMESTAMP_FIELD} field in schema, schema is {schema:?}"
                ))
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: None,
            routing_key_indices: None,
        })
    }

    pub fn from_schema_keys(schema: Arc<Schema>, key_indices: Vec<usize>) -> DFResult<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no {TIMESTAMP_FIELD} field in schema, schema is {schema:?}"
                ))
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
            routing_key_indices: None,
        })
    }

    pub fn schema_without_timestamp(&self) -> Schema {
        let mut builder = SchemaBuilder::from(self.schema.fields());
        builder.remove(self.timestamp_index);
        builder.finish()
    }

    pub fn remove_timestamp_column(&self, batch: &mut RecordBatch) {
        batch.remove_column(self.timestamp_index);
    }

    pub fn builders(&self) -> Vec<Box<dyn ArrayBuilder>> {
        self.schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 8))
            .collect()
    }

    pub fn timestamp_column<'a>(&self, batch: &'a RecordBatch) -> &'a TimestampNanosecondArray {
        batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
    }

    pub fn has_routing_keys(&self) -> bool {
        self.routing_keys().map(|k| !k.is_empty()).unwrap_or(false)
    }

    pub fn routing_keys(&self) -> Option<&Vec<usize>> {
        self.routing_key_indices
            .as_ref()
            .or(self.key_indices.as_ref())
    }

    pub fn storage_keys(&self) -> Option<&Vec<usize>> {
        self.key_indices.as_ref()
    }

    pub fn filter_by_time(
        &self,
        batch: RecordBatch,
        cutoff: Option<SystemTime>,
    ) -> Result<RecordBatch, ArrowError> {
        let Some(cutoff) = cutoff else {
            // no watermark, so we just return the same batch.
            return Ok(batch);
        };
        // filter out late data
        let timestamp_column = batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| ArrowError::CastError(
                format!("failed to downcast column {} of {:?} to timestamp. Schema is supposed to be {:?}",
                        self.timestamp_index, batch, self.schema)))?;
        let cutoff_scalar = TimestampNanosecondArray::new_scalar(to_nanos(cutoff) as i64);
        let on_time = gt_eq(timestamp_column, &cutoff_scalar)?;
        filter_record_batch(&batch, &on_time)
    }

    pub fn sort_columns(&self, batch: &RecordBatch, with_timestamp: bool) -> Vec<SortColumn> {
        let mut columns = vec![];
        if let Some(keys) = &self.key_indices {
            columns.extend(keys.iter().map(|index| SortColumn {
                values: batch.column(*index).clone(),
                options: None,
            }));
        }
        if with_timestamp {
            columns.push(SortColumn {
                values: batch.column(self.timestamp_index).clone(),
                options: None,
            });
        }
        columns
    }

    pub fn sort_fields(&self, with_timestamp: bool) -> Vec<SortField> {
        let mut sort_fields = vec![];
        if let Some(keys) = &self.key_indices {
            sort_fields.extend(keys.iter());
        }
        if with_timestamp {
            sort_fields.push(self.timestamp_index);
        }
        self.sort_fields_by_indices(&sort_fields)
    }

    fn sort_fields_by_indices(&self, indices: &[usize]) -> Vec<SortField> {
        indices
            .iter()
            .map(|index| SortField::new(self.schema.field(*index).data_type().clone()))
            .collect()
    }

    pub fn converter(&self, with_timestamp: bool) -> Result<Converter, ArrowError> {
        Converter::new(self.sort_fields(with_timestamp))
    }

    pub fn value_converter(
        &self,
        with_timestamp: bool,
        generation_index: usize,
    ) -> Result<Converter, ArrowError> {
        match &self.key_indices {
            None => {
                let mut indices = (0..self.schema.fields().len()).collect::<Vec<_>>();
                indices.remove(generation_index);
                if !with_timestamp {
                    indices.remove(self.timestamp_index);
                }
                Converter::new(self.sort_fields_by_indices(&indices))
            }
            Some(keys) => {
                let indices = (0..self.schema.fields().len())
                    .filter(|index| {
                        !keys.contains(index)
                            && (with_timestamp || *index != self.timestamp_index)
                            && *index != generation_index
                    })
                    .collect::<Vec<_>>();
                Converter::new(self.sort_fields_by_indices(&indices))
            }
        }
    }

    pub fn value_indices(&self, with_timestamp: bool) -> Vec<usize> {
        let field_count = self.schema.fields().len();
        match &self.key_indices {
            None => {
                let mut indices = (0..field_count).collect::<Vec<_>>();

                if !with_timestamp {
                    indices.remove(self.timestamp_index);
                }
                indices
            }
            Some(keys) => (0..field_count)
                .filter(|index| {
                    !keys.contains(index) && (with_timestamp || *index != self.timestamp_index)
                })
                .collect::<Vec<_>>(),
        }
    }

    pub fn sort(
        &self,
        batch: RecordBatch,
        with_timestamp: bool,
    ) -> Result<RecordBatch, ArrowError> {
        if self.key_indices.is_none() && !with_timestamp {
            return Ok(batch);
        }
        let sort_columns = self.sort_columns(&batch, with_timestamp);
        let sort_indices = lexsort_to_indices(&sort_columns, None).expect("should be able to sort");
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &sort_indices, None).unwrap())
            .collect();

        RecordBatch::try_new(batch.schema(), columns)
    }

    pub fn partition(
        &self,
        batch: &RecordBatch,
        with_timestamp: bool,
    ) -> Result<Vec<Range<usize>>, ArrowError> {
        if self.key_indices.is_none() && !with_timestamp {
            #[allow(clippy::single_range_in_vec_init)]
            return Ok(vec![0..batch.num_rows()]);
        }

        let mut partition_columns = vec![];

        if let Some(keys) = &self.routing_keys() {
            partition_columns.extend(keys.iter().map(|index| batch.column(*index).clone()));
        }
        if with_timestamp {
            partition_columns.push(batch.column(self.timestamp_index).clone());
        }

        Ok(partition(&partition_columns)?.ranges())
    }

    pub fn unkeyed_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        if self.key_indices.is_none() {
            return Ok(batch.clone());
        }
        let columns: Vec<_> = (0..batch.num_columns())
            .filter(|index| !self.key_indices.as_ref().unwrap().contains(index))
            .collect();
        batch.project(&columns)
    }

    pub fn schema_without_keys(&self) -> Result<Self, ArrowError> {
        if self.key_indices.is_none() {
            return Ok(self.clone());
        }
        let key_indices = self.key_indices.as_ref().unwrap();
        let unkeyed_schema = Schema::new(
            self.schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(index, _field)| !key_indices.contains(index))
                .map(|(_, field)| field.as_ref().clone())
                .collect::<Vec<_>>(),
        );
        let timestamp_index = unkeyed_schema.index_of(TIMESTAMP_FIELD)?;
        Ok(Self {
            schema: Arc::new(unkeyed_schema),
            timestamp_index,
            key_indices: None,
            routing_key_indices: None,
        })
    }

    pub fn with_fields(&self, fields: Vec<FieldRef>) -> Result<Self, ArrowError> {
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata.clone(),
        ));

        let timestamp_index = schema.index_of(TIMESTAMP_FIELD)?;
        let max_index = *[&self.key_indices, &self.routing_key_indices]
            .iter()
            .map(|indices| indices.as_ref().and_then(|k| k.iter().max()))
            .max()
            .flatten()
            .unwrap_or(&0);

        if schema.fields.len() - 1 < max_index {
            return Err(ArrowError::InvalidArgumentError(format!(
                "expected at least {} fields, but were only {}",
                max_index + 1,
                schema.fields.len()
            )));
        }

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: self.key_indices.clone(),
            routing_key_indices: self.routing_key_indices.clone(),
        })
    }

    pub fn with_additional_fields(
        &self,
        new_fields: impl Iterator<Item = Field>,
    ) -> Result<Self, ArrowError> {
        let mut fields = self.schema.fields.to_vec();
        fields.extend(new_fields.map(Arc::new));

        self.with_fields(fields)
    }
}

pub fn server_for_hash_array(
    hash: &PrimitiveArray<UInt64Type>,
    n: usize,
) -> Result<PrimitiveArray<UInt64Type>, ArrowError> {
    let range_size = u64::MAX / (n as u64) + 1;
    let range_scalar = UInt64Array::new_scalar(range_size);
    let division = div(hash, &range_scalar)?;
    let result: &PrimitiveArray<UInt64Type> = division.as_any().downcast_ref().unwrap();
    Ok(result.clone())
}

#[cfg(test)]
mod session_layout_tests {
    use super::*;
    #[test]
    fn session_source_admission_preserves_native_marker_clock_and_metadata_layout() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Utf8, true),
                Field::new(crate::SOURCE_ENVELOPE_FIELD, DataType::UInt64, false),
                Field::new(
                    TIMESTAMP_FIELD,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ],
            [("source".into(), "ordinary".into())].into_iter().collect(),
        ));
        let input = ArroyoSchema::from_schema_keys(schema.clone(), vec![]).unwrap();
        let expected = ArroyoSchema::new_unkeyed(Arc::new(schema.project(&[0, 2]).unwrap()), 1);
        assert_eq!(
            source_admission_output_schema(&input, Some(1), false).unwrap(),
            expected
        );
        assert_eq!(
            source_admission_output_schema(&input, Some(1), true).unwrap(),
            ArroyoSchema::new_unkeyed(schema, 2)
        );
        // Even without a marker, native watermark output is unkeyed (None),
        // whereas scalar lowering may have explicit empty storage keys.
        let plain = ArroyoSchema::from_schema_keys(expected.schema.clone(), vec![]).unwrap();
        assert_eq!(
            source_admission_output_schema(&plain, None, false).unwrap(),
            expected
        );
        assert!(source_admission_output_schema(&input, Some(0), false).is_err());
        assert!(source_admission_output_schema(&input, Some(2), false).is_err());
        assert!(source_admission_output_schema(&input, Some(99), false).is_err());
        assert!(source_admission_output_schema(&input, None, true).is_err());
        let mut bad_clock = input.clone();
        bad_clock.timestamp_index = usize::MAX;
        assert!(source_admission_output_schema(&bad_clock, Some(1), false).is_err());
        let mut fields = input.schema.fields().to_vec();
        fields[1] = Arc::new(Field::new(
            crate::SOURCE_ENVELOPE_FIELD,
            DataType::UInt64,
            true,
        ));
        let forged = ArroyoSchema::new_unkeyed(Arc::new(Schema::new(fields)), 2);
        assert!(source_admission_output_schema(&forged, Some(1), false).is_err());
    }
    fn schemas() -> (ArroyoSchema, Schema, ArroyoSchema) {
        let input = ArroyoSchema::from_schema_keys(
            Arc::new(Schema::new(vec![
                Field::new("__key_0", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
                Field::new(
                    TIMESTAMP_FIELD,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),
            vec![0],
        )
        .unwrap();
        let aggregate = Schema::new(vec![Field::new("count", DataType::Int64, false)]);
        let output = ArroyoSchema::from_schema_keys(
            Arc::new(Schema::new(vec![
                Field::new("public_key", DataType::Utf8, false),
                Field::new("window", window_arrow_struct(), true),
                aggregate.field(0).clone(),
                Field::new(
                    TIMESTAMP_FIELD,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),
            vec![],
        )
        .unwrap();
        (input, aggregate, output)
    }
    #[test]
    fn session_layout_preserves_public_key_alias_and_exact_native_fields() {
        let (input, aggregate, output) = schemas();
        assert_eq!(
            session_output_schema(&input, &aggregate, "window", 1, &output).unwrap(),
            output
        );
        let mut forged = output.clone();
        forged.timestamp_index = usize::MAX;
        assert_ne!(
            session_output_schema(&input, &aggregate, "window", 1, &forged).unwrap(),
            forged
        );
        assert!(session_output_schema(&input, &aggregate, "window", usize::MAX, &output).is_err());
        let mut input = input;
        input.timestamp_index = usize::MAX;
        assert!(session_output_schema(&input, &aggregate, "window", 1, &output).is_err());
    }
    #[test]
    fn session_layout_rejects_changed_native_key_type_nullability_and_metadata() {
        let (input, aggregate, output) = schemas();
        for field in [
            Field::new("public_key", DataType::Int64, false),
            Field::new("public_key", DataType::Utf8, true),
            Field::new("public_key", DataType::Utf8, false)
                .with_metadata([("changed".into(), "true".into())].into_iter().collect()),
        ] {
            let mut fields = output.schema.fields().to_vec();
            fields[0] = Arc::new(field);
            let forged = ArroyoSchema::new(
                Arc::new(Schema::new(fields)),
                output.timestamp_index,
                output.storage_keys().cloned(),
                output.routing_keys().cloned(),
            );
            assert!(session_output_schema(&input, &aggregate, "window", 1, &forged).is_err());
        }
    }
}
