//! Verified DataFusion accumulator footprints for native lifecycle adapters.
//! Eligibility requires the concrete built-in implementation and its state layout;
//! a fixed-looking UDAF schema does not establish bounded accumulator ownership.
use arrow_schema::{DataType, Schema};
use datafusion::functions_aggregate::{
    average::Avg,
    count::Count,
    median::Median,
    min_max::{Max, Min},
    regr::Regr,
    stddev::{Stddev, StddevPop},
    sum::Sum,
    variance::{VariancePopulation, VarianceSample},
};
use datafusion::{common::Result, physical_expr::aggregate::AggregateFunctionExpr};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateCapability {
    Fixed { state_bytes: usize },
    LinearReplay { bytes_per_row: usize },
}

fn numeric_width(data_type: &DataType) -> Option<usize> {
    Some(match data_type {
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => 8,
        DataType::Decimal128(_, _) => 16,
        DataType::Decimal256(_, _) => 32,
        _ => return None,
    })
}

/// Extrema retain their existing indexed/append-only lifecycle; a sliding
/// MIN/MAX accumulator owns a moving queue and cannot be treated as fixed.
pub fn is_extremum(aggregate: &AggregateFunctionExpr) -> bool {
    let function = aggregate.fun().inner().as_any();
    !aggregate.is_distinct()
        && aggregate.order_bys().is_none_or(|order| order.is_empty())
        && (function.is::<Min>() || function.is::<Max>())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AggregateDescriptor {
    pub capability: AggregateCapability,
    pub state_fields: usize,
    pub state_list_column: Option<usize>,
    pub fixed_bytes: usize,
    pub bytes_per_row: usize,
    pub payload_multiplier: usize,
    pub allocation_floor: usize,
    pub hash_entry_bytes: usize,
    pub minimum_entries: usize,
    pub digest_max_size: Option<usize>,
    pub hll_registers: bool,
    pub payload_is_max: bool,
    pub sliding_sum: bool,
}
impl AggregateDescriptor {
    /// SUM's verified sliding accumulator adds a UInt64 count to its ordinary
    /// one-value state. Bind that actual codec layout without dropping a field
    /// or copying lifecycle-specific footprint arithmetic into the worker.
    pub fn with_state_layout(mut self, fields: &[arrow_schema::FieldRef]) -> Result<Self> {
        use datafusion::common::exec_err;
        if fields.len() == self.state_fields {
            return Ok(self);
        }
        let AggregateCapability::Fixed { state_bytes: width } = self.capability else {
            return exec_err!("aggregate state layout changed");
        };
        if !self.sliding_sum
            || self.state_fields != 1
            || fields.len() != 2
            || numeric_width(fields[0].data_type()) != Some(width)
            || fields[1].data_type() != &DataType::UInt64
        {
            return exec_err!("aggregate state layout changed");
        }
        let Some(wrappers) = state_wrappers(fields) else {
            return exec_err!("aggregate state wrapper size overflow");
        };
        let Some(payload) = width.checked_add(8) else {
            return exec_err!("aggregate state payload size overflow");
        };
        let Some(fixed) = payload
            .checked_mul(4)
            .and_then(|payload| wrappers.checked_add(payload))
        else {
            return exec_err!("aggregate state size overflow");
        };
        self.fixed_bytes = fixed;
        self.state_fields = fields.len();
        self.capability = AggregateCapability::Fixed {
            state_bytes: payload,
        };
        Ok(self)
    }

    /// Validate the state formats that upstream decodes with assertions before
    /// handing checkpoint/input arrays to its merge accumulator.
    pub fn validate_partial_state(
        self,
        arrays: &[arrow_array::ArrayRef],
        row: usize,
    ) -> Result<()> {
        use arrow_array::{Array, BinaryArray, Float64Array, ListArray, UInt64Array};
        use datafusion::common::exec_err;
        if arrays.len() != self.state_fields || arrays.iter().any(|array| array.len() <= row) {
            return exec_err!("invalid aggregate state field count or row index");
        }
        if self.hll_registers {
            let Some(array) = arrays
                .first()
                .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            else {
                return exec_err!("invalid HLL state type");
            };
            if array.is_null(row) || array.value(row).len() != 16384 {
                return exec_err!("invalid HLL register state length");
            }
        }
        if let Some(max_size) = self.digest_max_size {
            if arrays.len() != 6 {
                return exec_err!("invalid TDigest state field count");
            }
            let Some(size) = arrays[0].as_any().downcast_ref::<UInt64Array>() else {
                return exec_err!("invalid TDigest max_size state type");
            };
            let Some(count) = arrays[2].as_any().downcast_ref::<UInt64Array>() else {
                return exec_err!("invalid TDigest count state type");
            };
            if size.is_null(row) || count.is_null(row) || size.value(row) != max_size as u64 {
                return exec_err!("TDigest max_size state identity changed");
            }
            for column in [1, 3, 4] {
                let Some(array) = arrays[column].as_any().downcast_ref::<Float64Array>() else {
                    return exec_err!("invalid TDigest scalar state type");
                };
                if array.is_null(row) {
                    return exec_err!("null TDigest scalar state");
                }
            }
            let Some(list) = arrays[5].as_any().downcast_ref::<ListArray>() else {
                return exec_err!("invalid TDigest centroids state type");
            };
            if list.is_null(row) {
                return exec_err!("null TDigest centroid list");
            }
            // TDigest::from_scalar_state reads List.values() (the entire
            // backing child), not the selected row span. Require a compact
            // single-row state so the checked/budgeted child is exactly what
            // upstream will consume; reject hidden prefix/suffix children.
            if row != 0
                || list.len() != 1
                || list.value_offsets()[0] != 0
                || usize::try_from(list.value_offsets()[1]).ok() != Some(list.values().len())
            {
                return exec_err!("TDigest centroid list has unselected backing children");
            }
            let values = list.values();
            if values.data_type() != &DataType::Float64
                || values.null_count() != 0
                || values.len() % 2 != 0
            {
                return exec_err!("invalid TDigest centroid pairs");
            }
            let max = arrays[3]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            let min = arrays[4]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            if max.is_finite() && min.is_finite() && max.total_cmp(&min).is_lt() {
                return exec_err!("invalid TDigest extrema state");
            }
        }
        Ok(())
    }
    pub fn retained_bound(self, rows: usize, payload: usize) -> Option<usize> {
        let buckets = if self.hash_entry_bytes == 0 {
            0
        } else {
            rows.max(self.minimum_entries)
                .checked_mul(8)?
                .checked_div(7)?
                .checked_add(1)?
                .checked_next_power_of_two()?
                .max(4)
        };
        // hashbrown control allocation includes the trailing16-byte SIMD
        // group, plus alignment padding; charge both tables during resize.
        let hash = if self.hash_entry_bytes == 0 {
            0
        } else {
            buckets
                .checked_mul(self.hash_entry_bytes.checked_add(1)?)?
                .checked_add(16 + std::mem::align_of::<u128>())?
                .checked_mul(2)?
        };
        self.fixed_bytes
            .checked_add(self.allocation_floor)?
            .checked_add(hash)?
            .checked_add(rows.checked_mul(self.bytes_per_row)?)?
            .checked_add(payload.checked_mul(self.payload_multiplier)?)
    }
}

/// Logical bytes copied by verified byte/string accumulators. This reads the
/// selected value without cloning it or charging unrelated shared backing.
pub fn logical_payload_bytes(array: &arrow_array::ArrayRef, row: usize) -> Result<usize> {
    use arrow_array::{
        Array, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray,
        StringViewArray,
    };
    use datafusion::common::exec_err;
    if row >= array.len() {
        return exec_err!("aggregate payload row index out of bounds");
    }
    if array.is_null(row) {
        return Ok(0);
    }
    macro_rules! bytes {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                return Ok(values.value(row).len());
            }
        };
    }
    bytes!(StringArray);
    bytes!(LargeStringArray);
    bytes!(StringViewArray);
    bytes!(BinaryArray);
    bytes!(LargeBinaryArray);
    bytes!(BinaryViewArray);
    exec_err!("aggregate byte payload type is not verified")
}

/// State scalar/Arrow wrappers remain live alongside accumulator ownership and
/// IPC output. Charge actual field count and alias bytes, including two scalar
/// and Arrow representations; never substitute one constant for six fields.
fn state_wrappers(fields: &[arrow_schema::FieldRef]) -> Option<usize> {
    use std::mem::size_of;
    let per_field = size_of::<arrow_schema::Field>()
        + 2 * size_of::<datafusion::common::ScalarValue>()
        + 2 * size_of::<arrow::array::ArrayData>()
        + 4 * size_of::<arrow_array::ArrayRef>();
    fields.iter().try_fold(
        size_of::<Vec<datafusion::common::ScalarValue>>(),
        |bytes, field| {
            let bytes = bytes
                .checked_add(per_field)?
                .checked_add(field.name().len().checked_mul(2)?)?;
            if let DataType::List(child) = field.data_type() {
                bytes
                    .checked_add(size_of::<arrow_schema::Field>())?
                    .checked_add(child.name().len().checked_mul(2)?)?
                    .checked_add(2 * size_of::<arrow::array::ArrayData>())
            } else {
                Some(bytes)
            }
        },
    )
}

fn literal_number(
    expr: &std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Option<f64> {
    use datafusion::common::ScalarValue;
    let literal = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Literal>()?;
    match literal.value() {
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => Some(*value),
        _ => None,
    }
}
fn literal_size(
    expr: &std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Option<usize> {
    use datafusion::common::ScalarValue;
    let literal = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Literal>()?;
    let value = match literal.value() {
        ScalarValue::UInt8(Some(value)) => u64::from(*value),
        ScalarValue::UInt16(Some(value)) => u64::from(*value),
        ScalarValue::UInt32(Some(value)) => u64::from(*value),
        ScalarValue::UInt64(Some(value)) => *value,
        ScalarValue::Int8(Some(value)) if *value > 0 => *value as u64,
        ScalarValue::Int16(Some(value)) if *value > 0 => *value as u64,
        ScalarValue::Int32(Some(value)) if *value > 0 => *value as u64,
        ScalarValue::Int64(Some(value)) if *value > 0 => *value as u64,
        _ => return None,
    };
    usize::try_from(value).ok().filter(|value| *value > 0)
}

pub fn aggregate_descriptor(
    aggregate: &AggregateFunctionExpr,
    schema: &Schema,
) -> Result<Option<AggregateDescriptor>> {
    use datafusion::functions_aggregate::{
        approx_distinct::ApproxDistinct,
        approx_median::ApproxMedian,
        approx_percentile_cont::ApproxPercentileCont,
        approx_percentile_cont_with_weight::ApproxPercentileContWithWeight,
        bool_and_or::{BoolAnd, BoolOr},
        correlation::Correlation,
        covariance::{CovariancePopulation, CovarianceSample},
    };
    let function = aggregate.fun().inner().as_any();
    let args = aggregate.expressions();
    let digest = function.is::<ApproxMedian>()
        || function.is::<ApproxPercentileCont>()
        || function.is::<ApproxPercentileContWithWeight>();
    if aggregate.order_bys().is_some_and(|order| !order.is_empty()) && !digest {
        return Ok(None);
    }
    let distinct_count = function.is::<Count>() && aggregate.is_distinct();
    if aggregate.is_distinct() && !distinct_count {
        return Ok(None);
    }
    let canonical_bits = [
        datafusion::functions_aggregate::bit_and_or_xor::bit_and_udaf(),
        datafusion::functions_aggregate::bit_and_or_xor::bit_or_udaf(),
        datafusion::functions_aggregate::bit_and_or_xor::bit_xor_udaf(),
    ];
    let bitwise = canonical_bits.iter().any(|builtin| {
        function.type_id() == builtin.inner().as_any().type_id()
            && aggregate.fun().name() == builtin.name()
    });
    let stats = function.is::<Stddev>()
        || function.is::<StddevPop>()
        || function.is::<VarianceSample>()
        || function.is::<VariancePopulation>();
    let regression = function.is::<Regr>() || function.is::<Correlation>();
    let covariance = function.is::<CovarianceSample>() || function.is::<CovariancePopulation>();
    let boolean = function.is::<BoolAnd>() || function.is::<BoolOr>();
    let hll = function.is::<ApproxDistinct>();
    let verified = function.is::<Count>()
        || function.is::<Sum>()
        || function.is::<Avg>()
        || function.is::<Min>()
        || function.is::<Max>()
        || bitwise;
    if !verified
        && !stats
        && !regression
        && !covariance
        && !boolean
        && !hll
        && !digest
        && !function.is::<Median>()
    {
        return Ok(None);
    }
    if ((stats || boolean || bitwise || hll) && args.len() != 1)
        || ((regression || covariance) && args.len() != 2)
    {
        return Ok(None);
    }
    let fields = aggregate.state_fields()?;
    let Some(wrappers) = state_wrappers(&fields) else {
        return Ok(None);
    };
    let mut descriptor = AggregateDescriptor {
        capability: AggregateCapability::Fixed { state_bytes: 0 },
        state_fields: fields.len(),
        state_list_column: None,
        fixed_bytes: wrappers,
        bytes_per_row: 0,
        payload_multiplier: 0,
        allocation_floor: 0,
        hash_entry_bytes: 0,
        minimum_entries: 0,
        digest_max_size: None,
        hll_registers: false,
        payload_is_max: false,
        sliding_sum: function.is::<Sum>(),
    };
    if distinct_count || function.is::<Median>() {
        if args.len() != 1 || fields.len() != 1 {
            return Ok(None);
        }
        let value_type = args[0].data_type(schema)?;
        if !matches!(fields[0].data_type(),DataType::List(child) if child.data_type()==&value_type)
        {
            return Ok(None);
        }
        let width = numeric_width(&value_type).filter(|_| {
            !distinct_count
                || matches!(
                    value_type,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                )
        });
        let text = matches!(
            value_type,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        );
        if distinct_count {
            if let Some(width) = width {
                // hashbrown buckets <=4*accepted cardinality; resizing overlaps
                // old/new tables. Include control bytes and Arrow state clones.
                descriptor.bytes_per_row = 4 * width;
                descriptor.hash_entry_bytes = width;
            } else if text {
                // binary_map.rs241..258: requested128-entry table and8192B
                // values buffer. Entry is hash+offset+length (three words on
                // this target); include table resize overlap and buffer clones.
                let entry = 3 * std::mem::size_of::<usize>() + 1;
                descriptor.bytes_per_row = 4 * std::mem::size_of::<usize>();
                descriptor.hash_entry_bytes = entry - 1;
                descriptor.minimum_entries = 128;
                descriptor.allocation_floor = 4 * 8192;
                descriptor.payload_multiplier = 4;
                if matches!(value_type, DataType::Utf8View | DataType::BinaryView) {
                    // DF binary_view_map requests512 entries. Arrow55.2
                    // GenericByteViewBuilder::new allocates1024 u128 views;
                    // the null builder can own1024 validity bits. take() owns
                    // old/new builders together. next_size doubles8192 to the
                    // first16384B block; its slack is a constructor floor.
                    descriptor.minimum_entries = 512;
                    let builder_floor = 1024 * std::mem::size_of::<u128>() + 1024 / 8 + 2 * 8192;
                    descriptor.allocation_floor = 2 * builder_floor;
                    descriptor.bytes_per_row = 4 * std::mem::size_of::<u128>()
                        + 4 * (std::mem::size_of::<arrow::buffer::Buffer>()
                            + 2 * std::mem::size_of::<usize>())
                        + 2 * std::mem::size_of::<u64>()
                        + 1;
                    // A flushed block is charged to its fill or the next value
                    // that did not fit: at most two adjacent blocks per value,
                    // each <=twice that value/fill =>4*logicalpayload. Add two
                    // serialized/state bodies and two copying/resize bodies.
                    descriptor.payload_multiplier = 8;
                }
            } else {
                return Ok(None);
            }
        } else {
            let Some(width) = width else { return Ok(None) };
            descriptor.bytes_per_row = 4 * width;
            descriptor.allocation_floor = 4 * descriptor.bytes_per_row;
        }
        descriptor.capability = AggregateCapability::LinearReplay {
            bytes_per_row: descriptor.bytes_per_row,
        };
        descriptor.state_list_column = Some(0);
        return Ok(Some(descriptor));
    }
    if digest {
        let types = fields
            .iter()
            .map(|field| field.data_type())
            .collect::<Vec<_>>();
        if types.len() != 6
            || types[..5]
                != [
                    &DataType::UInt64,
                    &DataType::Float64,
                    &DataType::UInt64,
                    &DataType::Float64,
                    &DataType::Float64,
                ]
            || !matches!(types[5],DataType::List(child) if child.data_type()==&DataType::Float64)
        {
            return Ok(None);
        }
        let mut max_size = 100usize; // upstream DEFAULT_MAX_SIZE
        if function.is::<ApproxMedian>() {
            if args.len() != 1 {
                return Ok(None);
            }
        } else {
            let weighted = function.is::<ApproxPercentileContWithWeight>();
            let percentile_index = if weighted { 2 } else { 1 };
            if (weighted && args.len() != 3) || (!weighted && !(2..=3).contains(&args.len())) {
                return Ok(None);
            }
            let Some(percentile) = literal_number(&args[percentile_index]) else {
                return Ok(None);
            };
            if !percentile.is_finite() || !(0.0..=1.0).contains(&percentile) {
                return Ok(None);
            }
            if !weighted && args.len() == 3 {
                let Some(size) = literal_size(&args[2]) else {
                    return Ok(None);
                };
                max_size = size;
            }
            if weighted && numeric_width(&args[1].data_type(schema)?).is_none() {
                return Ok(None);
            }
        }
        if !matches!(
            args[0].data_type(schema)?,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
        ) {
            return Ok(None);
        }
        // TDigest916b45f merge_sorted_f64/merge_digests retain the old live
        // centroid allocation while constructing Vec::with_capacity(max_size),
        // then shrink_to_fit may overlap its replacement allocation. Those are
        // three named max_size floors. All input/merge/state scratch below is
        // charged against actual raw N or restored centroid C, never C<=max_size.
        let Some(floor) = max_size
            .checked_mul(2 * std::mem::size_of::<f64>())
            .and_then(|bytes| bytes.checked_mul(3))
        else {
            return Ok(None);
        };
        descriptor.allocation_floor = floor;
        descriptor.digest_max_size = Some(max_size);
        let centroid = 2 * std::mem::size_of::<f64>();
        let digest_header = std::mem::size_of::<Vec<[f64; 2]>>() + 5 * std::mem::size_of::<u64>();
        descriptor.bytes_per_row = 2*digest_header // weighted Vec<TDigest> growth
            +4*centroid // singleton/input centroids and merge/external-merge copies
            +8*std::mem::size_of::<f64>() // means/weights, sorting/conversion scratch
            +4*std::mem::size_of::<datafusion::common::ScalarValue>() // mean/weight scalar state and its clone
            +4*std::mem::size_of::<f64>(); // serialized mean/weight arrays and clone
        descriptor.capability = AggregateCapability::LinearReplay {
            bytes_per_row: descriptor.bytes_per_row,
        };
        descriptor.state_list_column = Some(5);
        return Ok(Some(descriptor));
    }
    let payload = if hll {
        if fields.len() != 1
            || fields[0].data_type() != &DataType::Binary
            || args.len() != 1
            || !matches!(
                args[0].data_type(schema)?,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::Binary
                    | DataType::LargeBinary
            )
        {
            return Ok(None);
        }
        descriptor.hll_registers = true;
        if matches!(
            args[0].data_type(schema)?,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
        ) {
            descriptor.payload_multiplier = 1; // upstream to_string/to_vec temporary before hashing
            descriptor.payload_is_max = true;
        }
        16384 // HLL register ownership, state/export/merge clones.
    } else if stats || regression || covariance {
        let expected = if regression {
            6
        } else if covariance {
            4
        } else {
            3
        };
        if fields.len() != expected
            || fields[0].data_type() != &DataType::UInt64
            || fields[1..]
                .iter()
                .any(|field| field.data_type() != &DataType::Float64)
        {
            return Ok(None);
        }
        expected * 8
    } else if boolean {
        if fields.len() != 1 || fields[0].data_type() != &DataType::Boolean {
            return Ok(None);
        }
        1
    } else {
        if fields.is_empty() || (bitwise && fields.len() != 1) {
            return Ok(None);
        }
        let Some(widths) = fields
            .iter()
            .map(|field| numeric_width(field.data_type()))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        widths.iter().sum()
    };
    descriptor.fixed_bytes = descriptor
        .fixed_bytes
        .saturating_add(payload.saturating_mul(4));
    descriptor.capability = AggregateCapability::Fixed {
        state_bytes: payload,
    };
    Ok(Some(descriptor))
}

pub fn classify_aggregate(
    aggregate: &AggregateFunctionExpr,
    schema: &Schema,
) -> Result<Option<AggregateCapability>> {
    Ok(aggregate_descriptor(aggregate, schema)?.map(|descriptor| descriptor.capability))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::{
        logical_expr::function::AccumulatorArgs,
        logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility},
        physical_expr::{aggregate::AggregateExprBuilder, expressions::Column},
    };
    use std::{any::Any, sync::Arc};

    #[derive(Debug)]
    struct NamedMedian(Signature);
    impl AggregateUDFImpl for NamedMedian {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "median"
        }
        fn signature(&self) -> &Signature {
            &self.0
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }
        fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
            unreachable!("capability checks must not instantiate opaque accumulators")
        }
    }
    #[test]
    fn verified_layout_and_concrete_implementation_are_both_required() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let build = |fun| {
            AggregateExprBuilder::new(fun, vec![Arc::new(Column::new("value", 0))])
                .schema(schema.clone())
                .alias("result")
                .build()
                .unwrap()
        };
        let median = build(datafusion::functions_aggregate::median::median_udaf());
        assert_eq!(
            classify_aggregate(&median, &schema).unwrap(),
            Some(AggregateCapability::LinearReplay { bytes_per_row: 32 })
        );
        let descriptor = aggregate_descriptor(&median, &schema).unwrap().unwrap();
        assert!(descriptor.retained_bound(0, 0).unwrap() > 4 * 32);
        assert_eq!(
            descriptor.retained_bound(8, 0).unwrap() - descriptor.retained_bound(0, 0).unwrap(),
            8 * 32
        );
        assert!(descriptor.retained_bound(usize::MAX, 0).is_none());
        let fake = build(Arc::new(AggregateUDF::new_from_impl(NamedMedian(
            Signature::any(1, Volatility::Immutable),
        ))));
        assert_eq!(classify_aggregate(&fake, &schema).unwrap(), None);
        let sum = build(datafusion::functions_aggregate::sum::sum_udaf());
        let ordinary = aggregate_descriptor(&sum, &schema).unwrap().unwrap();
        let sliding = ordinary
            .with_state_layout(&sum.sliding_state_fields().unwrap())
            .unwrap();
        assert_eq!(sliding.state_fields, 2);
        assert_eq!(
            sliding.capability,
            AggregateCapability::Fixed { state_bytes: 16 }
        );
        assert!(sliding.retained_bound(0, 0).unwrap() > ordinary.retained_bound(0, 0).unwrap());
        let stddev = build(datafusion::functions_aggregate::stddev::stddev_udaf());
        assert_eq!(
            classify_aggregate(&stddev, &schema).unwrap(),
            Some(AggregateCapability::Fixed { state_bytes: 24 })
        );
    }
    #[test]
    fn descriptor_preserves_digest_order_and_validates_state_before_upstream_decode() {
        use arrow_array::{ArrayRef, Float64Array};
        use datafusion::{
            common::ScalarValue,
            physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Literal},
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let build = |percentile: f64, descending: bool| {
            AggregateExprBuilder::new(
            datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf(),
            vec![Arc::new(Column::new("value",0)),Arc::new(Literal::new(ScalarValue::Float64(Some(percentile)))),
                Arc::new(Literal::new(ScalarValue::UInt64(Some(10))))])
            .schema(schema.clone()).alias("digest")
            .order_by(LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("value",0)),
                arrow_schema::SortOptions {descending,nulls_first:false})])).build().unwrap()
        };
        let descending = build(0.8, true);
        let ascending = build(0.2, false);
        let descriptor = aggregate_descriptor(&descending, &schema).unwrap().unwrap();
        assert_eq!(descriptor.digest_max_size, Some(10));
        assert_eq!(descriptor.state_list_column, Some(5));
        assert!(
            descriptor.fixed_bytes > 6 * std::mem::size_of::<datafusion::common::ScalarValue>()
        );
        let values = Arc::new(Float64Array::from_iter_values((0..257).map(f64::from))) as ArrayRef;
        let mut desc = descending.create_accumulator().unwrap();
        let mut asc = ascending.create_accumulator().unwrap();
        desc.update_batch(std::slice::from_ref(&values)).unwrap();
        asc.update_batch(&[values]).unwrap();
        let desc_value = desc.evaluate().unwrap();
        let asc_value = asc.evaluate().unwrap();
        let (ScalarValue::Float64(Some(left)), ScalarValue::Float64(Some(right))) =
            (desc_value, asc_value)
        else {
            panic!("unexpected digest value")
        };
        assert!((left - right).abs() < 1e-10);
        let state = desc.state().unwrap();
        let arrays = state
            .iter()
            .map(|value| value.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();
        descriptor.validate_partial_state(&arrays, 0).unwrap();
        assert!(descriptor.validate_partial_state(&arrays, 1).is_err());
        assert!(descriptor.validate_partial_state(&arrays[..5], 0).is_err());
        let mut malformed = arrays.clone();
        malformed[0] = ScalarValue::UInt64(Some(11)).to_array_of_size(1).unwrap();
        assert!(descriptor.validate_partial_state(&malformed, 0).is_err());
        let odd =
            ScalarValue::new_list_nullable(&[ScalarValue::Float64(Some(1.))], &DataType::Float64);
        malformed = arrays;
        malformed[5] = ScalarValue::List(odd).to_array_of_size(1).unwrap();
        assert!(descriptor.validate_partial_state(&malformed, 0).is_err());
        let mut builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Float64Builder::new());
        for _ in 0..2 {
            builder.values().append_value(1.);
            builder.values().append_value(1.);
            builder.append(true);
        }
        let shared = builder.finish().slice(1, 1);
        malformed[5] = Arc::new(shared);
        assert!(descriptor.validate_partial_state(&malformed, 0).is_err());
        let compact = arrow::compute::take(
            &malformed[5],
            &arrow_array::UInt32Array::from(vec![0]),
            None,
        )
        .unwrap();
        malformed[5] = compact;
        descriptor.validate_partial_state(&malformed, 0).unwrap();
        assert!(descriptor.retained_bound(257, 0).unwrap() >= desc.size());
    }

    #[test]
    fn count_distinct_text_constructor_floor_and_hll_register_shape_are_explicit() {
        use datafusion::common::ScalarValue;
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let build = |fun, distinct| {
            let builder = AggregateExprBuilder::new(fun, vec![Arc::new(Column::new("value", 0))])
                .schema(schema.clone())
                .alias("state");
            if distinct {
                builder.distinct().build().unwrap()
            } else {
                builder.build().unwrap()
            }
        };
        let count = build(datafusion::functions_aggregate::count::count_udaf(), true);
        let descriptor = aggregate_descriptor(&count, &schema).unwrap().unwrap();
        assert_eq!(descriptor.minimum_entries, 128);
        assert!(descriptor.retained_bound(0, 0).unwrap() > 4 * 8192);
        assert!(descriptor.retained_bound(usize::MAX, 0).is_none());
        let hll = build(
            datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf(),
            false,
        );
        let hll = aggregate_descriptor(&hll, &schema).unwrap().unwrap();
        assert!(hll.hll_registers);
        assert!(hll.retained_bound(0, 0).unwrap() > 16384);
        let good = ScalarValue::Binary(Some(vec![0; 16384]))
            .to_array_of_size(1)
            .unwrap();
        hll.validate_partial_state(&[good], 0).unwrap();
        let bad = ScalarValue::Binary(Some(vec![0; 16383]))
            .to_array_of_size(1)
            .unwrap();
        assert!(hll.validate_partial_state(&[bad], 0).is_err());
    }
}
