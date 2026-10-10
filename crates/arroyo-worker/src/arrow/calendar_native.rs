//! Calendar membership inside the existing native aggregate owner. Each bucket
//! is ordinary typed DataFusion COUNT/SUM state; lifetime state stays in G.
//! Buckets are retained until group retirement. Raw references may move backwards,
//! so an arrival date alone never authorizes deleting contribution history.
use super::*;
use crate::arrow::aggregate_codec::{decode_calendar_contribution, encode_calendar_contribution};
use arrow_array::Date32Array;
use arroyo_rpc::grpc::api::CalendarAggregateDescriptor;

pub(super) struct CalendarContributionChange<'a> {
    pub group: &'a [u8],
    pub generation: u64,
    pub index: usize,
    pub row_id: Option<&'a [u8]>,
    pub retract: bool,
    pub selected: Option<Vec<ArrayRef>>,
    pub day: Option<i32>,
    pub argument_types: Vec<DataType>,
}

#[derive(Debug)]
pub(super) struct CalendarAggregate {
    pub aggregate_index: usize,
    pub storage_index: usize,
    contribution: Arc<dyn PhysicalExpr>,
    reference: Arc<dyn PhysicalExpr>,
    horizon: i32,
}

pub(super) struct CalendarInput {
    pub contribution: Date32Array,
    pub reference: Date32Array,
}

impl CalendarInput {
    pub fn contribution_day(&self, row: usize) -> Result<Option<i32>> {
        Ok((!self.contribution.is_null(row)).then(|| self.contribution.value(row)))
    }
    pub fn reference_day(&self, row: usize) -> Result<i32> {
        ensure!(
            !self.reference.is_null(row),
            "calendar FILTER reference date cannot be NULL"
        );
        Ok(self.reference.value(row))
    }
}

impl CalendarAggregate {
    #[cfg(test)]
    pub(super) fn test_dates(&mut self, contribution: i32, reference: i32) {
        self.contribution = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            ScalarValue::Date32(Some(contribution)),
        ));
        self.reference = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            ScalarValue::Date32(Some(reference)),
        ));
    }
    pub fn decode(
        descriptors: &[CalendarAggregateDescriptor],
        aggregates: &[Aggregator],
        schema: &Schema,
        registry: &Registry,
    ) -> Result<Vec<Self>> {
        let mut indices = HashSet::new();
        let mut families = HashMap::new();
        descriptors
            .iter()
            .map(|descriptor| {
                let index = usize::try_from(descriptor.aggregate_index)?;
                ensure!(
                    indices.insert(index),
                    "duplicate calendar aggregate ordinal"
                );
                let aggregate = aggregates
                    .get(index)
                    .context("calendar aggregate ordinal is invalid")?;
                ensure!(
                    aggregate.accumulator_type == AccumulatorType::Sliding
                        && matches!(
                            aggregate.func.fun().name().to_ascii_lowercase().as_str(),
                            "count" | "sum"
                        )
                        && !aggregate.func.is_distinct()
                        && aggregate.input_exprs.len() == 1,
                    "calendar FILTER supports non-distinct COUNT/SUM with one argument"
                );
                ensure!(
                    descriptor.horizon_days > 0,
                    "calendar horizon must be positive"
                );
                ensure!(
                    !descriptor.context_id.is_empty(),
                    "calendar FILTER context identity is missing"
                );
                let decode = |bytes: &[u8]| -> Result<Arc<dyn PhysicalExpr>> {
                    let expression = parse_physical_expr(
                        &PhysicalExprNode::decode(bytes)?,
                        registry,
                        schema,
                        &DefaultPhysicalExtensionCodec {},
                    )?;
                    ensure!(
                        expression.data_type(schema)? == DataType::Date32,
                        "calendar FILTER date expression must return Date32"
                    );
                    Ok(expression)
                };
                let codec = DefaultPhysicalExtensionCodec {};
                let argument = datafusion_proto::physical_plan::to_proto::serialize_physical_expr(
                    &aggregate.input_exprs[0],
                    &codec,
                )?
                .encode_to_vec();
                let filter = aggregate
                    .filter
                    .as_ref()
                    .map(|filter| {
                        datafusion_proto::physical_plan::to_proto::serialize_physical_expr(
                            filter, &codec,
                        )
                        .map(|node| node.encode_to_vec())
                    })
                    .transpose()?;
                ensure!(
                    argument == descriptor.argument,
                    "calendar descriptor argument does not match aggregate input"
                );
                ensure!(
                    filter == descriptor.static_filter,
                    "calendar descriptor static filter does not match aggregate FILTER"
                );
                let family = (
                    aggregate.func.fun().name().to_ascii_lowercase(),
                    descriptor.argument.clone(),
                    descriptor.static_filter.clone(),
                    descriptor.contribution_date.clone(),
                );
                let storage_index = *families.entry(family).or_insert(index);
                Ok(Self {
                    aggregate_index: index,
                    storage_index,
                    contribution: decode(&descriptor.contribution_date)?,
                    reference: decode(&descriptor.reference_date)?,
                    horizon: i32::try_from(descriptor.horizon_days)
                        .context("calendar horizon exceeds Date32 range")?,
                })
            })
            .collect()
    }
}

fn family_prefix(kind: u8, group: &[u8], generation: u64, aggregate: usize) -> Result<Vec<u8>> {
    let mut key = native_generation_prefix(kind, group, generation)?;
    key.extend_from_slice(&u32::try_from(aggregate)?.to_be_bytes());
    Ok(key)
}

fn bucket_key(group: &[u8], generation: u64, aggregate: usize, day: i32) -> Result<Vec<u8>> {
    let mut key = family_prefix(b'B', group, generation, aggregate)?;
    // Signed days sort in calendar order, including dates before Unix epoch.
    key.extend_from_slice(&((day as u32) ^ (1 << 31)).to_be_bytes());
    Ok(key)
}

pub(super) fn calendar_due_key(deadline: i64, group: &[u8]) -> Result<Vec<u8>> {
    let mut key = vec![b'H'];
    key.extend_from_slice(&((deadline as u64) ^ (1 << 63)).to_be_bytes());
    key.extend_from_slice(&u32::try_from(group.len())?.to_be_bytes());
    key.extend_from_slice(group);
    Ok(key)
}

impl IncrementalAggregatingFunc {
    pub(super) fn calendar_write_operations(&self) -> usize {
        if self.calendars.is_empty() {
            return 0;
        }
        let families: HashSet<_> = self
            .calendars
            .iter()
            .map(|calendar| calendar.storage_index)
            .collect();
        let non_calendar_sliding = self
            .aggregates
            .iter()
            .enumerate()
            .filter(|(index, aggregate)| {
                aggregate.accumulator_type == AccumulatorType::Sliding
                    && !self
                        .calendars
                        .iter()
                        .any(|calendar| calendar.aggregate_index == *index)
            })
            .count();
        families
            .len()
            .saturating_add(self.calendars.len())
            .saturating_add(3)
            .saturating_add(if self.native_append_only {
                0
            } else {
                families.len() + non_calendar_sliding
            })
    }

    pub(super) fn calendar_inputs(&self, batch: &RecordBatch) -> Result<Vec<CalendarInput>> {
        self.calendars
            .iter()
            .map(|calendar| {
                let evaluate = |expression: &Arc<dyn PhysicalExpr>| -> Result<Date32Array> {
                    let array = expression.evaluate(batch)?.into_array(batch.num_rows())?;
                    array
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .cloned()
                        .context("calendar expression did not return Date32")
                };
                let contribution = evaluate(&calendar.contribution)?;
                let reference = evaluate(&calendar.reference)?;
                ensure!(
                    reference.null_count() == 0,
                    "calendar FILTER reference date cannot be NULL"
                );
                for day in reference.values() {
                    day.checked_sub(calendar.horizon - 1)
                        .context("calendar reference horizon exceeds Date32 range")?;
                    Self::calendar_next_boundary(*day)?;
                }
                Ok(CalendarInput {
                    contribution,
                    reference,
                })
            })
            .collect()
    }

    // One signed correction uses the original gate/day/value, even when the CDC
    // before row carries a new envelope clock. Ordinary immutable sources have
    // no per-event ledger. Entries survive membership expiry and lifetime uses
    // the same original arguments independently of recent buckets.
    pub(super) async fn calendar_original_input(
        &self,
        scope: &mut AggregateScope<'_>,
        change: CalendarContributionChange<'_>,
    ) -> Result<(Option<Vec<ArrayRef>>, Option<i32>)> {
        let CalendarContributionChange {
            group,
            generation,
            index,
            row_id,
            retract,
            selected,
            day,
            argument_types,
        } = change;
        let Some(id) = row_id else {
            return Ok((selected, day));
        };
        let count = self.aggregates[index]
            .func
            .fun()
            .name()
            .eq_ignore_ascii_case("count");
        // COUNT needs the original null flags, never the original payload. The
        // existing COUNT accumulator accepts any array type and uses only its
        // length/null count. SUM/AVG retain their original typed numeric values.
        let stored_types = if count {
            vec![DataType::Boolean]
        } else {
            argument_types
        };
        let mut key = family_prefix(b'J', group, generation, index)?;
        key.extend_from_slice(id);
        if retract {
            let bytes = scope
                .get(&key)
                .await?
                .context("calendar aggregate original contribution is missing")?;
            let (original, original_day) =
                decode_calendar_contribution(&bytes, &stored_types, scope.limits().value_bytes)?;
            scope.delete(&key)?;
            Ok((original, original_day))
        } else {
            ensure!(
                scope.get(&key).await?.is_none(),
                "calendar aggregate insert requires an absent row identity; corrections must retract the original contribution"
            );
            let count_flags = count
                .then(|| {
                    selected.as_ref().map(|values| {
                        vec![Arc::new(BooleanArray::from(vec![
                            (!values.iter().any(|value| value.is_null(0))).then_some(true),
                        ])) as ArrayRef]
                    })
                })
                .flatten();
            let stored_values = if count {
                count_flags.as_deref()
            } else {
                selected.as_deref()
            };
            let bytes = encode_calendar_contribution(
                stored_values,
                day,
                &stored_types,
                scope.limits().value_bytes,
            )?;
            scope.put(&key, &bytes)?;
            Ok((selected, day))
        }
    }

    fn calendar_state_types(&self, calendar: &CalendarAggregate) -> Result<Vec<DataType>> {
        Ok(self.aggregates[calendar.aggregate_index]
            .func
            .sliding_state_fields()?
            .iter()
            .map(|field| field.data_type().clone())
            .collect())
    }

    pub(super) async fn calendar_bucket_delta(
        &self,
        scope: &mut AggregateScope<'_>,
        calendar: &CalendarAggregate,
        day: i32,
        change: NativeMemberChange<'_>,
    ) -> Result<()> {
        let NativeMemberChange {
            group,
            generation,
            values,
            retract,
            ..
        } = change;
        let aggregate = &self.aggregates[calendar.aggregate_index];
        let key = bucket_key(group, generation, calendar.storage_index, day)?;
        let mut accumulator = aggregate.func.create_sliding_accumulator()?;
        if let Some(bytes) = scope.get(&key).await? {
            let state = decode_group(
                &bytes,
                &self.calendar_state_types(calendar)?,
                &[],
                scope.limits().value_bytes,
            )?;
            accumulator.merge_batch(
                &state
                    .accumulator_state
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<DFResult<Vec<_>>>()?,
            )?;
        } else {
            ensure!(
                !retract,
                "calendar aggregate retracts a missing contribution bucket"
            );
        }
        if retract {
            accumulator.retract_batch(values)?;
        } else {
            accumulator.update_batch(values)?;
        }
        let state = EncodedGroup {
            last_update_nanos: 0,
            generation,
            next_ordinal: 0,
            accumulator_state: accumulator.state()?,
            last_emitted: None,
        };
        scope.put(&key, &encode_group(&state, scope.limits().value_bytes)?)?;
        Ok(())
    }

    pub(super) fn calendar_set_reference(
        &self,
        scope: &mut AggregateScope<'_>,
        group: &[u8],
        generation: u64,
        calendar: &CalendarAggregate,
        reference: i32,
    ) -> Result<()> {
        scope.put(
            &family_prefix(b'Q', group, generation, calendar.aggregate_index)?,
            &reference.to_be_bytes(),
        )
    }

    pub(super) async fn calendar_recalculate(
        &self,
        scope: &AggregateScope<'_>,
        group: &[u8],
        generation: u64,
        calendar: &CalendarAggregate,
        reference: i32,
    ) -> Result<IncrementalState> {
        let aggregate = &self.aggregates[calendar.aggregate_index];
        let mut accumulator = aggregate.func.create_sliding_accumulator()?;
        let first_day = reference
            .checked_sub(calendar.horizon - 1)
            .context("calendar reference horizon exceeds Date32 range")?;
        let prefix = family_prefix(b'B', group, generation, calendar.storage_index)?;
        let mut after = if first_day == i32::MIN {
            None
        } else {
            Some(bucket_key(
                group,
                generation,
                calendar.storage_index,
                first_day - 1,
            )?)
        };
        let last = bucket_key(group, generation, calendar.storage_index, reference)?;
        while let Some((key, bytes)) = scope.first_from(&prefix, after.as_deref()).await? {
            if key > last {
                break;
            }
            ensure!(key.len() == prefix.len() + 4, "invalid calendar bucket key");
            let state = decode_group(
                &bytes,
                &self.calendar_state_types(calendar)?,
                &[],
                scope.limits().value_bytes,
            )?;
            accumulator.merge_batch(
                &state
                    .accumulator_state
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<DFResult<Vec<_>>>()?,
            )?;
            after = Some(key);
        }
        Ok(IncrementalState::Sliding {
            accumulator,
            expr: aggregate.func.clone(),
        })
    }

    // H sorts signed UTC deadlines, U is the generation-owned reverse pointer.
    // Both use the owner's registered live table and checkpoint barriers.
    pub(super) async fn calendar_schedule(
        &self,
        scope: &mut AggregateScope<'_>,
        group: &[u8],
        generation: u64,
        reference: i32,
    ) -> Result<()> {
        let pointer = native_generation_prefix(b'U', group, generation)?;
        if let Some(bytes) = scope.get(&pointer).await? {
            ensure!(bytes.len() == 8, "invalid calendar due boundary");
            let previous = i64::from_be_bytes(bytes.as_slice().try_into()?);
            scope.delete(&calendar_due_key(previous, group)?)?;
        }
        let deadline = Self::calendar_next_boundary(reference)?;
        scope.put(
            &calendar_due_key(deadline, group)?,
            &generation.to_be_bytes(),
        )?;
        scope.put(&pointer, &deadline.to_be_bytes())?;
        Ok(())
    }

    /// STR-29's quiet-key traversal invokes this with the real progress UTC day.
    /// This callback never evaluates row-context functions using invented rows.
    /// The caller commits this owner's scope and drains D through normal output.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "STR-29 owns quiet-key scheduler integration")
    )]
    pub(crate) async fn recalculate_calendar_group(
        &self,
        scope: &mut AggregateScope<'_>,
        group_key: &[u8],
        reference: i32,
    ) -> Result<Option<i64>> {
        if self.calendars.is_empty() {
            return Ok(None);
        }
        let key = native_group_key(b'G', group_key)?;
        let Some(bytes) = scope.get(&key).await? else {
            return Ok(None);
        };
        let mut group = decode_group(
            &bytes,
            &self.native_state_types(),
            &self.native_output_types(),
            scope.limits().value_bytes,
        )?;
        let mut accumulators = self.native_accumulators(Some(&group))?;
        for calendar in &self.calendars {
            self.calendar_set_reference(scope, group_key, group.generation, calendar, reference)?;
            accumulators[calendar.aggregate_index] = self
                .calendar_recalculate(scope, group_key, group.generation, calendar, reference)
                .await?;
        }
        self.calendar_schedule(scope, group_key, group.generation, reference)
            .await?;
        group.accumulator_state = self.native_state_values(&mut accumulators)?;
        scope.put(&key, &encode_group(&group, scope.limits().value_bytes)?)?;
        scope.put(&native_group_key(b'D', group_key)?, &[1])?;
        Ok(Some(Self::calendar_next_boundary(reference)?))
    }

    pub(crate) fn calendar_next_boundary(reference: i32) -> Result<i64> {
        i64::from(reference)
            .checked_add(1)
            .and_then(|day| day.checked_mul(86_400_000_000_000))
            .context("calendar next UTC boundary exceeds timestamp range")
    }
}
