//! Calendar membership inside the existing native aggregate owner. Each bucket
//! is ordinary typed DataFusion COUNT/SUM state; lifetime state stays in G.
//! Real source watermark progress bounds retained days. Raw references never
//! authorize pruning; original correction metadata and lifetime survive expiry.
use super::*;
use crate::arrow::aggregate_codec::{
    CalendarCleanup, CalendarProgress, decode_calendar_contribution, encode_calendar_contribution,
};
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
    #[cfg(test)]
    pub(super) fn test_clock_reference(&mut self) {
        self.reference = Arc::new(datafusion::physical_expr::expressions::CastExpr::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new(
                TIMESTAMP_FIELD,
                3,
            )),
            DataType::Date32,
            None,
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
    fn calendar_progress_for(&self, watermark_nanos: i64) -> Result<CalendarProgress> {
        let horizon = self
            .calendars
            .iter()
            .map(|calendar| calendar.horizon)
            .max()
            .context("calendar horizon is missing")?;
        let day = i32::try_from(watermark_nanos.div_euclid(86_400_000_000_000))?;
        Ok(CalendarProgress {
            watermark_nanos,
            first_retained_day: day
                .checked_sub(horizon - 1)
                .context("calendar pruning boundary exceeds Date32 range")?,
        })
    }

    async fn calendar_progress(
        &self,
        scope: &AggregateScope<'_>,
    ) -> Result<Option<CalendarProgress>> {
        scope
            .get(b"W")
            .await?
            .map(|bytes| {
                let progress = CalendarProgress::decode(&bytes)?;
                ensure!(
                    progress == self.calendar_progress_for(progress.watermark_nanos)?,
                    "calendar pruning boundary does not match compiled horizon"
                );
                Ok(progress)
            })
            .transpose()
    }

    fn validate_calendar_reference(progress: CalendarProgress, reference: i32) -> Result<()> {
        let admitted_day = i32::try_from(progress.watermark_nanos.div_euclid(86_400_000_000_000))?;
        ensure!(
            reference >= admitted_day,
            "calendar recalculation reference precedes admitted watermark context"
        );
        Ok(())
    }

    pub(super) async fn validate_calendar_inputs(&self, inputs: &[CalendarInput]) -> Result<()> {
        if inputs.is_empty() {
            return Ok(());
        }
        let store = self
            .native_store
            .as_ref()
            .context("calendar aggregate store missing")?;
        let scope = store.begin().await?;
        if let Some(progress) = self.calendar_progress(&scope).await? {
            for input in inputs {
                for reference in input.reference.values() {
                    Self::validate_calendar_reference(progress, *reference)?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn calendar_watermark(
        &self,
        watermark: arroyo_types::Watermark,
    ) -> Result<()> {
        if self.calendars.is_empty() {
            return Ok(());
        }
        let arroyo_types::Watermark::EventTime(time) = watermark else {
            return Ok(());
        };
        // EOF is a drain signal outside Arrow's signed domain, never progress
        // authorizing history deletion. Idle similarly carries no event time.
        if time == arroyo_types::from_nanos(u64::MAX as u128) {
            return Ok(());
        }
        let nanos = arroyo_types::event_time::to_signed_nanos(time)
            .context("calendar watermark exceeds timestamp range")?;
        let store = self
            .native_store
            .as_ref()
            .context("calendar aggregate store missing")?;
        let mut scope = store.begin().await?;
        let prior = self.calendar_progress(&scope).await?;
        if prior.is_none_or(|prior| nanos > prior.watermark_nanos) {
            let progress = self.calendar_progress_for(nanos)?;
            scope.put(b"W", &progress.encode())?;
            let day = i32::try_from(nanos.div_euclid(86_400_000_000_000))?;
            if prior.is_none_or(|prior| {
                prior.watermark_nanos.div_euclid(86_400_000_000_000) < i64::from(day)
            }) {
                let cleanup = CalendarCleanup {
                    watermark_day: day,
                    after: None,
                    complete: false,
                };
                scope.put(b"V", &cleanup.encode(scope.limits().value_bytes)?)?;
            }
            scope.commit().await?;
        } else {
            drop(scope);
        }
        self.prune_calendar_buckets().await
    }

    fn calendar_family_floor(&self, progress: CalendarProgress, family: usize) -> Result<i32> {
        let horizon = self
            .calendars
            .iter()
            .filter(|calendar| calendar.storage_index == family)
            .map(|calendar| calendar.horizon)
            .max()
            .context("unknown calendar bucket family")?;
        i32::try_from(progress.watermark_nanos.div_euclid(86_400_000_000_000))?
            .checked_sub(horizon - 1)
            .context("calendar family pruning boundary exceeds Date32 range")
    }

    fn calendar_bucket_identity(key: &[u8]) -> Result<(usize, i32)> {
        ensure!(
            key.len() >= 21 && key[0] == b'B',
            "invalid calendar bucket key"
        );
        let group_bytes = usize::try_from(u32::from_be_bytes(key[1..5].try_into()?))?;
        ensure!(
            group_bytes.checked_add(21) == Some(key.len()),
            "invalid calendar bucket key width"
        );
        let family = usize::try_from(u32::from_be_bytes(
            key[key.len() - 8..key.len() - 4].try_into()?,
        ))?;
        let day = (u32::from_be_bytes(key[key.len() - 4..].try_into()?) ^ (1 << 31)) as i32;
        Ok((family, day))
    }

    /// Resume one bounded deletion page, including after checkpoint/recovery.
    /// V advances atomically with deletions; G and J remain untouched.
    pub(super) async fn prune_calendar_buckets(&self) -> Result<()> {
        if self.calendars.is_empty() {
            return Ok(());
        }
        let store = self
            .native_store
            .as_ref()
            .context("calendar aggregate store missing")?;
        let mut scope = store.begin().await?;
        let Some(progress) = self.calendar_progress(&scope).await? else {
            return Ok(());
        };
        let day = i32::try_from(progress.watermark_nanos.div_euclid(86_400_000_000_000))?;
        let mut cleanup = scope
            .get(b"V")
            .await?
            .map(|bytes| CalendarCleanup::decode(&bytes))
            .transpose()?
            .context("calendar watermark is missing cleanup frontier")?;
        ensure!(
            cleanup.watermark_day == day,
            "calendar cleanup frontier does not match watermark"
        );
        if cleanup.complete {
            return Ok(());
        }
        if let Some(after) = &cleanup.after {
            Self::calendar_bucket_identity(after)?;
        }
        let limits = scope.limits();
        // Reserve one operation and one maximum entry for persisted V progress.
        let page_entries = limits
            .page_entries
            .min(limits.write_operations.saturating_sub(1))
            .min((limits.write_bytes / store.max_encoded_entry_bytes()).saturating_sub(1))
            .min(
                (limits.overlay_bytes / (limits.key_bytes + limits.value_bytes)).saturating_sub(1),
            );
        ensure!(
            page_entries > 0,
            "calendar pruning cannot admit bucket and cursor progress"
        );
        for _ in 0..page_entries {
            let Some((key, _)) = scope.first_from(b"B", cleanup.after.as_deref()).await? else {
                cleanup.complete = true;
                cleanup.after = None;
                break;
            };
            let (family, contribution) = Self::calendar_bucket_identity(&key)?;
            if contribution < self.calendar_family_floor(progress, family)? {
                scope.delete(&key)?;
            }
            cleanup.after = Some(key);
        }
        scope.put(b"V", &cleanup.encode(limits.value_bytes)?)?;
        scope.commit().await
    }

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
        if let Some(progress) = self.calendar_progress(scope).await?
            && day < self.calendar_family_floor(progress, calendar.storage_index)?
        {
            // Original J still supplies the lifetime delta. Expired recent
            // membership must neither be resurrected nor require a lost bucket.
            return Ok(());
        }
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
            validity_deadline_nanos: 0,
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
        if let Some(progress) = self.calendar_progress(scope).await? {
            Self::validate_calendar_reference(progress, reference)?;
        }
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

    /// Recalculate at most one due group in an admitted scope. H and U are
    /// persisted together with G/D, so cancellation before commit leaves the
    /// pending boundary intact and checkpoint restore resumes the same work.
    pub(super) async fn advance_calendar_watermark(&self, watermark_nanos: i64) -> Result<bool> {
        if self.calendars.is_empty() {
            return Ok(false);
        }
        let store = self
            .native_store
            .as_ref()
            .context("native aggregate store missing")?;
        // Each reference plus old H deletion, new H/U and G/D. Reserve for
        // the entire owner mutation before starting; one group never spans
        // scopes and output remains in the existing bounded D drain.
        let operations = self
            .calendars
            .len()
            .checked_add(5)
            .context("calendar expiry operation budget overflow")?;
        ensure!(
            operations <= store.limits().write_operations
                && operations <= store.limits().write_bytes / store.max_encoded_entry_bytes()
                && operations
                    <= store.limits().overlay_bytes
                        / (store.limits().key_bytes + store.limits().value_bytes),
            "native aggregate budget cannot recalculate one calendar group"
        );
        let mut scope = store.begin().await?;
        // W is the checkpointed admission frontier. Regressing or restored
        // notifications cannot recalculate against an older date after its
        // history has been pruned. Direct callers without W retain their
        // supplied finite progress for existing focused operator probes.
        let watermark_nanos = self
            .calendar_progress(&scope)
            .await?
            .map_or(watermark_nanos, |progress| {
                progress.watermark_nanos.max(watermark_nanos)
            });
        let reference = i32::try_from(watermark_nanos.div_euclid(86_400_000_000_000))?;
        let Some((due, generation_bytes)) = scope.first(b"H").await? else {
            return Ok(false);
        };
        ensure!(
            due.len() >= 13 && generation_bytes.len() == 8,
            "invalid calendar due entry"
        );
        let deadline = (u64::from_be_bytes(due[1..9].try_into()?) ^ (1 << 63)) as i64;
        if deadline > watermark_nanos {
            return Ok(false);
        }
        let group_len = usize::try_from(u32::from_be_bytes(due[9..13].try_into()?))?;
        ensure!(
            group_len == due.len() - 13,
            "invalid calendar due group key"
        );
        let group_key = &due[13..];
        let generation = u64::from_be_bytes(generation_bytes.as_slice().try_into()?);
        let pointer = native_generation_prefix(b'U', group_key, generation)?;
        let owns_boundary = scope
            .get(&pointer)
            .await?
            .is_some_and(|value| value == deadline.to_be_bytes());
        let current = scope
            .get(&native_group_key(b'G', group_key)?)
            .await?
            .map(|bytes| {
                decode_group(
                    &bytes,
                    &self.native_state_types(),
                    &self.native_output_types(),
                    scope.limits().value_bytes,
                )
            })
            .transpose()?;
        if owns_boundary && current.is_some_and(|group| group.generation == generation) {
            self.recalculate_calendar_group(&mut scope, group_key, reference)
                .await?;
        } else {
            // Stale generations never recalculate a replacement key. Leave
            // reverse-pointer cleanup with its existing generation owner.
            scope.delete(&due)?;
        }
        scope.commit().await?;
        Ok(true)
    }

    /// STR-29's quiet-key traversal invokes this with the real progress UTC day.
    /// This callback never evaluates row-context functions using invented rows.
    /// The caller commits this owner's scope and drains D through normal output.
    pub(crate) async fn recalculate_calendar_group(
        &self,
        scope: &mut AggregateScope<'_>,
        group_key: &[u8],
        reference: i32,
    ) -> Result<Option<i64>> {
        if self.calendars.is_empty() {
            return Ok(None);
        }
        // Validate the entire callback context before Q/G/due-index writes.
        if let Some(progress) = self.calendar_progress(scope).await? {
            Self::validate_calendar_reference(progress, reference)?;
        }
        Self::calendar_next_boundary(reference)?;
        for calendar in &self.calendars {
            reference
                .checked_sub(calendar.horizon - 1)
                .context("calendar reference horizon exceeds Date32 range")?;
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
