// Included inside incremental_aggregator::tests so this finite diagnostic uses
// the existing private native constructors and callbacks, without product hooks.
use super::*;

const DRAIN_DAY_NS: i64 = 86_400_000_000_000;
const DRAIN_D0: i32 = 20_666; // 2026-08-01 UTC (Date32).
const DRAIN_KEYS: usize = 1024;

// Exactly two test-only process snapshots, outside the rollover callback and
// wall timer. CPU deltas include the small read/print boundary overhead. HWM is
// process-lifetime high water (including fixture preparation), not rollover peak.
fn drain_diagnostic_process_sample(phase: &str) -> Result<()> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .get(stat.rfind(')').context("invalid diagnostic process stat")? + 2..)
        .context("truncated diagnostic process stat")?
        .split_whitespace()
        .collect::<Vec<_>>();
    ensure!(
        fields.len() > 19,
        "truncated diagnostic process stat fields"
    );
    let user = fields[11].parse::<u64>()?;
    let system = fields[12].parse::<u64>()?;
    let start = fields[19].parse::<u64>()?;
    let status = std::fs::read_to_string("/proc/self/status")?;
    let io = std::fs::read_to_string("/proc/self/io")?;
    let number = |text: &str, key: &str| -> Result<u64> {
        let line = text
            .lines()
            .find(|line| line.starts_with(key))
            .with_context(|| format!("missing diagnostic process field {key}"))?;
        Ok(line[key.len()..]
            .split_whitespace()
            .next()
            .context("missing diagnostic process field value")?
            .parse()?)
    };
    let rss = number(&status, "VmRSS:")?;
    let hwm = number(&status, "VmHWM:")?;
    eprintln!(
        "CALENDAR_DRAIN_PROCESS phase={phase} pid={} start_ticks={start} stat_user_ticks={user} stat_system_ticks={system} rss_kib={rss} hwm_kib={hwm} rchar={} wchar={} syscr={} syscw={} read_bytes={} write_bytes={} cancelled_write_bytes={} hwm_scope=whole_process planned_samples=2 boundary_overhead=included",
        std::process::id(),
        number(&io, "rchar:")?,
        number(&io, "wchar:")?,
        number(&io, "syscr:")?,
        number(&io, "syscw:")?,
        number(&io, "read_bytes:")?,
        number(&io, "write_bytes:")?,
        number(&io, "cancelled_write_bytes:")?,
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DrainOracleRow {
    key: String,
    retract: bool,
    values: [i64; 5],
    clock: i64,
}

#[derive(Default)]
struct DrainDiagnosticCollector {
    rows: Vec<DrainOracleRow>,
}

#[async_trait::async_trait]
impl Collector for DrainDiagnosticCollector {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("diagnostic output key is not Utf8")?;
        let retracts = IncrementalAggregatingFunc::get_retracts(&batch)
            .context("missing diagnostic retract metadata")?;
        let columns = (1..=5)
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("diagnostic COUNT output is not Int64")
            })
            .collect::<Result<Vec<_>>>()?;
        let clock = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .context("diagnostic output clock is not TimestampNanosecond")?;
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || clock.is_null(row)
                || columns.iter().any(|column| column.is_null(row))
            {
                return Err(anyhow!("NULL diagnostic output").into());
            }
            self.rows.push(DrainOracleRow {
                key: keys.value(row).to_owned(),
                retract: retracts.value(row),
                values: std::array::from_fn(|index| columns[index].value(row)),
                clock: clock.value(row),
            });
        }
        Ok(())
    }

    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}

pub(super) fn drain_diagnostic_config() -> Result<(
    UpdatingAggregateOperator,
    Arc<Registry>,
    AggregateStateConfig,
)> {
    use arroyo_rpc::grpc::api::CalendarAggregateDescriptor;
    use datafusion::physical_expr::expressions::CastExpr;
    let (mut config, registry) = native_config();
    let mut plan = PhysicalPlanNode::decode(config.aggregate_exec.as_slice())?;
    let Some(PhysicalPlanType::Aggregate(aggregate)) = &mut plan.physical_plan_type else {
        bail!("diagnostic requires native aggregate plan");
    };
    let lifetime = aggregate.aggr_expr[5].clone();
    let calendar = aggregate.aggr_expr[6].clone();
    let clock = aggregate.aggr_expr[8].clone();
    let filter = aggregate.filter_expr[6].clone();
    aggregate.aggr_expr = vec![
        lifetime,
        calendar.clone(),
        calendar.clone(),
        calendar.clone(),
        calendar,
        clock,
    ];
    aggregate.aggr_expr_name = [
        "lifetime",
        "recent_1",
        "recent_7",
        "recent_30",
        "recent_90",
        TIMESTAMP_FIELD,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    aggregate.filter_expr = vec![
        MaybeFilter { expr: None },
        filter.clone(),
        filter.clone(),
        filter.clone(),
        filter.clone(),
        MaybeFilter { expr: None },
    ];
    config.aggregate_exec = plan.encode_to_vec();
    let input = ArroyoSchema::new(input_schema(), 3, Some(vec![0]), Some(vec![0]));
    config.input_schema = Some(input.into());
    let mut fields = vec![input_schema().field(0).clone()];
    fields.extend(
        ["lifetime", "recent_1", "recent_7", "recent_30", "recent_90"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int64, false)),
    );
    fields.push(Field::new(
        TIMESTAMP_FIELD,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ));
    fields.push(Field::new(
        UPDATING_META_FIELD,
        DataType::Struct(updating_meta_fields()),
        false,
    ));
    config.final_schema = Some(
        ArroyoSchema::new(
            Arc::new(Schema::new(fields)),
            6,
            Some(vec![0]),
            Some(vec![0]),
        )
        .into(),
    );
    config.retain_indefinitely = Some(true);
    let codec = DefaultPhysicalExtensionCodec {};
    let date: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        Arc::new(Column::new(TIMESTAMP_FIELD, 3)),
        DataType::Date32,
        None,
    ));
    let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
    config.calendar_aggregates = [1, 7, 30, 90]
        .into_iter()
        .enumerate()
        .map(|(index, horizon)| {
            Ok(CalendarAggregateDescriptor {
                aggregate_index: (index + 1) as u32,
                horizon_days: horizon,
                argument: serialize_physical_expr(&one, &codec)?.encode_to_vec(),
                static_filter: filter.expr.as_ref().map(prost::Message::encode_to_vec),
                contribution_date: serialize_physical_expr(&date, &codec)?.encode_to_vec(),
                reference_date: serialize_physical_expr(&date, &codec)?.encode_to_vec(),
                context_id: "opaque-clock-rollover-diagnostic".into(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((
        config,
        registry,
        AggregateStateConfig {
            key_bytes: 512,
            value_bytes: 32768,
            page_bytes: 131072,
            page_entries: 64,
            write_bytes: 2097152,
            write_operations: 128,
            overlay_bytes: 2097152,
            max_pending_output_rows: 64,
            max_pending_output_bytes: 524288,
            max_resident_bytes: 134217728,
        },
    ))
}

fn drain_seed_clock(day: usize, round: usize, key: usize) -> i64 {
    (i64::from(DRAIN_D0) + day as i64) * DRAIN_DAY_NS
        + DRAIN_DAY_NS / 2
        + ((round * DRAIN_KEYS + key) as i64) * 1_000_000
}

fn drain_seed_include(day: usize, round: usize, key: usize) -> Option<bool> {
    if day == 1 && key == 0 {
        return round.is_multiple_of(2).then_some(false);
    }
    if day == 1 && key == 1 && round < 2 {
        return (round == 0).then_some(false);
    }
    Some(true)
}

// Independent literal-event oracle; does not decode product state to calculate
// expected counts. The last timestamp remains the last accepted input clock.
fn drain_expected(day: usize, round: usize, key: usize, reference: i32) -> DrainOracleRow {
    let mut values = [0; 5];
    for input_day in 0..=day {
        let rounds = if input_day == day { round + 1 } else { 9 };
        for input_round in 0..rounds {
            values[0] += 1;
            if drain_seed_include(input_day, input_round, key) == Some(true) {
                for (position, horizon) in [1, 7, 30, 90].into_iter().enumerate() {
                    let contribution = DRAIN_D0 + input_day as i32;
                    if contribution <= reference && contribution > reference - horizon {
                        values[position + 1] += 1;
                    }
                }
            }
        }
    }
    DrainOracleRow {
        key: format!("q{key:04}"),
        retract: false,
        values,
        clock: drain_seed_clock(day, round, key),
    }
}

async fn drain_diagnostic_processing(
    operator: &mut IncrementalAggregatingFunc,
    ctx: &mut OperatorContext,
) -> Result<()> {
    let started = std::time::Instant::now();
    let mut previous = std::collections::BTreeMap::<String, DrainOracleRow>::new();
    let mut collector = DrainDiagnosticCollector::default();
    let mut inputs = 0usize;
    eprintln!("CALENDAR_DRAIN_PHASE phase=prepare_start inputs=0 quiet_owners=1024");
    for day in 0..2 {
        for round in 0..9 {
            let names = (0..DRAIN_KEYS)
                .map(|key| format!("q{key:04}"))
                .collect::<Vec<_>>();
            let clocks = (0..DRAIN_KEYS)
                .map(|key| drain_seed_clock(day, round, key))
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                input_schema(),
                vec![
                    Arc::new(StringArray::from(
                        names.iter().map(String::as_str).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(clocks.clone())),
                    Arc::new(BooleanArray::from(
                        (0..DRAIN_KEYS)
                            .map(|key| drain_seed_include(day, round, key))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(TimestampNanosecondArray::from(clocks)),
                ],
            )?;
            operator.process_batch(batch, ctx, &mut collector).await?;
            inputs += DRAIN_KEYS;
            ensure!(
                collector.rows.is_empty(),
                "ordinary input unexpectedly emitted before flush"
            );
            operator
                .handle_tick((day * 9 + round) as u64, ctx, &mut collector)
                .await?;
            let mut expected = Vec::new();
            for key in 0..DRAIN_KEYS {
                let next = drain_expected(day, round, key, DRAIN_D0 + day as i32);
                if let Some(prior) = previous.insert(next.key.clone(), next.clone()) {
                    expected.push(DrainOracleRow {
                        retract: true,
                        ..prior
                    });
                }
                expected.push(next);
            }
            ensure!(
                collector.rows == expected,
                "seed CDC/order oracle mismatch day={day} round={round}"
            );
            eprintln!(
                "CALENDAR_DRAIN_PHASE phase=seed_flush day={day} round={round} inputs={inputs} cdc_rows={} exact=true elapsed_s={:.9}",
                collector.rows.len(),
                started.elapsed().as_secs_f64()
            );
            collector.rows.clear();
        }
        let watermark = drain_seed_clock(day, 8, DRAIN_KEYS - 1) - 5_000_000_000;
        operator
            .handle_watermark(
                Watermark::EventTime(
                    arroyo_types::event_time::from_signed_nanos(watermark)
                        .context("seed watermark outside SystemTime")?,
                ),
                ctx,
                &mut collector,
            )
            .await?;
        ensure!(
            collector.rows.is_empty(),
            "same-day seed watermark changed CDC"
        );
    }
    let driver_clock = i64::from(DRAIN_D0 + 2) * DRAIN_DAY_NS + 5_001_000_000;
    operator
        .process_batch(
            batch_at(&[Some("d0000")], &[driver_clock], &[driver_clock]),
            ctx,
            &mut collector,
        )
        .await?;
    inputs += 1;
    operator.handle_tick(18, ctx, &mut collector).await?;
    ensure!(
        collector.rows
            == vec![DrainOracleRow {
                key: "d0000".into(),
                retract: false,
                values: [1; 5],
                clock: driver_clock
            }],
        "driver CDC oracle mismatch"
    );
    collector.rows.clear();
    ensure!(inputs == 18_433, "fixture input denominator changed");
    let watermark = driver_clock - 5_000_000_000;
    eprintln!(
        "CALENDAR_DRAIN_PHASE phase=rollover_start inputs={inputs} quiet_owners=1024 watermark_ns={watermark}"
    );
    drain_diagnostic_process_sample("rollover_start")?;
    let rollover = std::time::Instant::now();
    operator
        .handle_watermark(
            Watermark::EventTime(
                arroyo_types::event_time::from_signed_nanos(watermark)
                    .context("rollover watermark outside SystemTime")?,
            ),
            ctx,
            &mut collector,
        )
        .await?;
    let seconds = rollover.elapsed().as_secs_f64();
    drain_diagnostic_process_sample("rollover_end")?;
    let mut expected = Vec::new();
    for key in 1..DRAIN_KEYS {
        let mut prior = previous
            .get(&format!("q{key:04}"))
            .context("missing prior oracle")?
            .clone();
        prior.retract = true;
        expected.push(prior);
        expected.push(drain_expected(1, 8, key, DRAIN_D0 + 2));
    }
    ensure!(
        collector.rows == expected,
        "rollover exact old/new/order oracle mismatch"
    );
    ensure!(
        collector.rows.len() == 2046,
        "rollover CDC denominator changed"
    );
    eprintln!(
        "CALENDAR_DRAIN_PHASE phase=rollover_end seconds={seconds:.9} returned=true cdc_rows=2046 changed_owners=1023 suppressed_owners=1 exact=true"
    );

    // Untimed, bounded caller-known-key verification. No state enumeration.
    let store = operator
        .native_store
        .as_ref()
        .context("diagnostic store missing")?;
    let mut verified = 0;
    for key in 0..DRAIN_KEYS {
        let names: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![format!("q{key:04}")]))];
        let rows = operator.key_converter.convert_columns(&names)?;
        let group_key = rows.row(0);
        let scope = store.begin_point().await?;
        let bytes = scope
            .get(&native_group_key(b'G', group_key.as_ref())?)
            .await?
            .context("missing quiet G")?;
        let group = operator
            .native_group_codec()?
            .decode(&bytes, scope.limits().value_bytes)?;
        for position in 0..4 {
            ensure!(
                group.calendar_references.get(position)? == Some(DRAIN_D0 + 2),
                "quiet reference not advanced"
            );
        }
        let due = i64::from(DRAIN_D0 + 3) * DRAIN_DAY_NS;
        let pointer = scope
            .get(&native_generation_prefix(
                b'U',
                group_key.as_ref(),
                group.generation,
            )?)
            .await?
            .context("missing quiet U")?;
        ensure!(
            pointer == due.to_be_bytes(),
            "quiet due pointer not advanced"
        );
        let scheduled = scope
            .get(&calendar_native::calendar_due_key(due, group_key.as_ref())?)
            .await?
            .context("missing quiet H")?;
        ensure!(
            scheduled == group.generation.to_be_bytes(),
            "quiet H generation mismatch"
        );
        ensure!(
            scope
                .get(&native_group_key(b'D', group_key.as_ref())?)
                .await?
                .is_none(),
            "quiet D remains after completed drain"
        );
        let expected = drain_expected(1, 8, key, DRAIN_D0 + 2);
        let values = operator
            .native_accepted_values(&scope, group_key.as_ref(), &group)
            .await?;
        ensure!(
            values[..5] == expected.values.map(|value| ScalarValue::Int64(Some(value))),
            "known-key final COUNT mismatch"
        );
        verified += 1;
    }
    eprintln!(
        "CALENDAR_DRAIN_RESULT inputs={inputs} verified_quiet_owners={verified} cdc_rows=2046 exact=true elapsed_s={:.9}",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit isolated retained-state calendar day-rollover scan baseline"]
async fn calendar_day_rollover_scan_baseline() -> Result<()> {
    use arroyo_state::live::lifecycle::RocksStateConfig;
    use arroyo_state::live::worker::{BackendConstruction, construct_backend};
    ensure!(
        std::env::var_os("STREAMR_LIVE_SCAN_DIAGNOSTICS").as_deref()
            == Some(std::ffi::OsStr::new("1")),
        "baseline requires existing scan diagnostics"
    );
    let root = std::env::temp_dir().join(format!(
        "calendar-drain-diagnostic-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root)?;
    let resources = WorkerStateResources::new(ResourceConfig {
        block_cache_bytes: 268435456,
        memtable_bytes: 67108864,
        queued_write_bytes: 100663296,
        decoded_value_bytes: 33554432,
        scan_page_bytes: 8388608,
        max_blocking_operations: 2,
        max_snapshots: 4,
        max_open_databases: 8,
        disk_reserve_bytes: 67108864,
    })?;
    let backend = construct_backend(
        BackendConstruction::Rocksdb(RocksStateConfig {
            root: root.clone(),
            job_id: "calendar-drain-diagnostic".into(),
            operator_id: "opaque-owner".into(),
            subtask: 0,
            generation: 0,
            attempt: 0,
        }),
        resources.clone(),
    )
    .await?;
    eprintln!(
        "CALENDAR_DRAIN_CONTROLS root={} profile=dev-test durability=worker-checkpoint wal=false sync=false quiet_keys=1024 inputs=18433 block_cache=268435456 shared_wbm=67108864 decoded=33554432 scan_page=8388608 blocking=2",
        root.display()
    );
    // All setup after backend ownership is established is inside the outcome
    // envelope too. An error, unwind, or timeout drops its manager/operator/
    // context handles before the common close joins remaining native work.
    let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
        tokio::time::timeout(Duration::from_secs(600), async {
            let mut manager = LiveTableManager::new(
                backend.clone(),
                Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
            )?;
            let table = manager.register(NATIVE_AGGREGATE_TABLE)?;
            let (config, registry, native) = drain_diagnostic_config()?;
            let input: ArroyoSchema = config
                .input_schema
                .clone()
                .context("missing diagnostic input")?
                .try_into()?;
            let output: ArroyoSchema = config
                .final_schema
                .clone()
                .context("missing diagnostic output")?
                .try_into()?;
            let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
                config,
                registry,
                Some(native),
            )?;
            operator.native_store = Some(AggregateStore::new(
                backend.clone(),
                table,
                resources,
                AggregateStoreLimits {
                    key_bytes: native.key_bytes,
                    value_bytes: native.value_bytes,
                    page_bytes: native.page_bytes,
                    page_entries: native.page_entries,
                    write_bytes: native.write_bytes,
                    write_operations: native.write_operations,
                    overlay_bytes: native.overlay_bytes,
                },
            )?);
            let mut ctx = calendar_dispatch_context(&mut operator).await;
            ctx.in_schemas = vec![Arc::new(input)];
            ctx.out_schema = Some(Arc::new(output));
            drain_diagnostic_processing(&mut operator, &mut ctx).await
        }),
    ))
    .await;
    tokio::time::timeout(Duration::from_secs(30), backend.close())
        .await
        .context("diagnostic backend cleanup exceeded 30s")??;
    std::fs::remove_dir_all(&root)?;
    eprintln!("CALENDAR_DRAIN_CLEANUP closed=true");
    match result {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(anyhow!("finite baseline processing limit: {error}")),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
