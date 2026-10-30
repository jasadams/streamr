use super::*;
use arroyo_state::live::{
    self, LastEntryRequest, ReadOptions, ScanPage, ScanRequest, SnapshotReader, StateKey,
    StateSnapshot, WriteBatch,
    worker::{BackendConstruction, construct_backend},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const KNOWN_DAY: i32 = 20_666;
const KNOWN_DAY_NS: i64 = 86_400_000_000_000;

#[derive(Default)]
struct KnownTrace {
    dirty_gets: AtomicUsize,
    dirty_scans: AtomicUsize,
    snapshots: AtomicUsize,
    suspend_dirty_get: AtomicBool,
    suspended: AtomicUsize,
}

struct KnownReader {
    inner: StateSnapshot,
    trace: Arc<KnownTrace>,
}

impl Drop for KnownReader {
    fn drop(&mut self) {
        self.trace.snapshots.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl SnapshotReader for KnownReader {
    async fn get(&self, key: &StateKey, options: ReadOptions) -> live::Result<Option<Vec<u8>>> {
        self.try_get(key, options).await
    }
    async fn try_get(&self, key: &StateKey, options: ReadOptions) -> live::Result<Option<Vec<u8>>> {
        if key.key.first() == Some(&b'D') {
            self.trace.dirty_gets.fetch_add(1, Ordering::SeqCst);
            if self.trace.suspend_dirty_get.swap(false, Ordering::SeqCst) {
                self.trace.suspended.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
        }
        self.inner.try_get(key, options).await
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> live::Result<Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys, options).await
    }
    async fn scan(&self, request: ScanRequest) -> live::Result<ScanPage> {
        self.try_scan(request).await
    }
    async fn try_scan(&self, request: ScanRequest) -> live::Result<ScanPage> {
        if request.range.prefix.as_deref() == Some(b"D") {
            self.trace.dirty_scans.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.try_scan(request).await
    }
    async fn last_in_range(&self, request: LastEntryRequest) -> live::Result<ScanPage> {
        self.inner.last_in_range(request).await
    }
}

struct KnownBackend {
    inner: Arc<dyn LiveStateBackend>,
    trace: Arc<KnownTrace>,
}

#[async_trait::async_trait]
impl LiveStateBackend for KnownBackend {
    async fn get(&self, key: &StateKey, options: ReadOptions) -> live::Result<Option<Vec<u8>>> {
        self.inner.get(key, options).await
    }
    async fn try_get(&self, key: &StateKey, options: ReadOptions) -> live::Result<Option<Vec<u8>>> {
        self.inner.try_get(key, options).await
    }
    async fn multi_get(
        &self,
        keys: &[StateKey],
        options: ReadOptions,
    ) -> live::Result<Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys, options).await
    }
    async fn write_batch(&self, batch: WriteBatch) -> live::Result<()> {
        self.inner.write_batch(batch).await
    }
    async fn snapshot(&self) -> live::Result<StateSnapshot> {
        let inner = self.inner.snapshot().await?;
        self.trace.snapshots.fetch_add(1, Ordering::SeqCst);
        Ok(StateSnapshot(Arc::new(KnownReader {
            inner,
            trace: self.trace.clone(),
        })))
    }
    fn try_admit_write(&self, bytes: usize, operations: usize) -> live::Result<AdmittedWriteBatch> {
        self.inner.try_admit_write(bytes, operations)
    }
    async fn admit_write(
        &self,
        bytes: usize,
        operations: usize,
    ) -> live::Result<AdmittedWriteBatch> {
        self.inner.admit_write(bytes, operations).await
    }
    async fn write_admitted(&self, batch: AdmittedWriteBatch) -> live::Result<()> {
        self.inner.write_admitted(batch).await
    }
}

struct KnownFixture {
    operator: IncrementalAggregatingFunc,
    ctx: OperatorContext,
    backend: Arc<dyn LiveStateBackend>,
    resources: WorkerStateResources,
    trace: Arc<KnownTrace>,
    root: std::path::PathBuf,
}

impl KnownFixture {
    async fn new(rocks: bool, fallback: bool) -> Self {
        let (config, registry, native) =
            super::calendar_drain_diagnostic_tests::drain_diagnostic_config().unwrap();
        let input: ArroyoSchema = config.input_schema.clone().unwrap().try_into().unwrap();
        let output: ArroyoSchema = config.final_schema.clone().unwrap().try_into().unwrap();
        let root = std::env::temp_dir().join(format!("known-calendar-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let mut control = ResourceConfig {
            block_cache_bytes: 268435456,
            memtable_bytes: 67108864,
            queued_write_bytes: 100663296,
            decoded_value_bytes: 33554432,
            scan_page_bytes: 8388608,
            max_blocking_operations: 2,
            max_snapshots: 4,
            max_open_databases: 8,
            disk_reserve_bytes: 67108864,
        };
        if fallback {
            let namespace = StateNamespace {
                ownership: Ownership::PartitionLocal {
                    subtask: 0,
                    parallelism: 1,
                },
                table: NATIVE_AGGREGATE_TABLE.as_bytes().to_vec(),
            };
            let entry = aggregate_encoded_key_bound(&namespace, native.key_bytes).unwrap()
                + native.value_bytes
                + 1;
            // Exactly the existing mandatory workspace, with no space for
            // the optional logical-progress charge. Ordinary drain must fit.
            control.decoded_value_bytes = native.overlay_bytes
                + 3 * native.value_bytes
                + 3 * native.max_pending_output_bytes
                + 2 * entry
                + 128;
        }
        let resources = WorkerStateResources::new(control).unwrap();
        let construction = if rocks {
            BackendConstruction::Rocksdb(live::lifecycle::RocksStateConfig {
                root: root.clone(),
                job_id: "known-calendar".into(),
                operator_id: "opaque".into(),
                subtask: 0,
                generation: 0,
                attempt: 0,
            })
        } else {
            BackendConstruction::Memory {
                max_resident_bytes: native.max_resident_bytes,
            }
        };
        let backend = construct_backend(construction, resources.clone())
            .await
            .unwrap();
        let trace = Arc::new(KnownTrace::default());
        let wrapped: Arc<dyn LiveStateBackend> = Arc::new(KnownBackend {
            inner: backend.clone(),
            trace: trace.clone(),
        });
        let mut manager = LiveTableManager::new(
            wrapped.clone(),
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1,
            },
        )
        .unwrap();
        let table = manager.register(NATIVE_AGGREGATE_TABLE).unwrap();
        let mut operator = IncrementalAggregatingConstructor::build_with_native_config(
            config,
            registry,
            Some(native),
        )
        .unwrap();
        operator.native_store = Some(
            AggregateStore::new(
                wrapped,
                table,
                resources.clone(),
                AggregateStoreLimits {
                    key_bytes: native.key_bytes,
                    value_bytes: native.value_bytes,
                    page_bytes: native.page_bytes,
                    page_entries: native.page_entries,
                    write_bytes: native.write_bytes,
                    write_operations: native.write_operations,
                    overlay_bytes: native.overlay_bytes,
                },
            )
            .unwrap(),
        );
        let mut ctx = calendar_dispatch_context(&mut operator).await;
        ctx.in_schemas = vec![Arc::new(input)];
        ctx.out_schema = Some(Arc::new(output));
        drop(manager);
        let progress = operator
            .native_store
            .as_ref()
            .unwrap()
            .try_expiry_progress(native.max_pending_output_bytes);
        assert_eq!(progress.is_none(), fallback);
        drop(progress);
        Self {
            operator,
            ctx,
            backend,
            resources,
            trace,
            root,
        }
    }

    async fn seed(&mut self, names: &[&str], day: i32, include: &[Option<bool>]) {
        self.seed_pending(names, day, include).await;
        let mut out = AggregateCollector::default();
        self.operator
            .handle_tick(0, &mut self.ctx, &mut out)
            .await
            .unwrap();
    }

    async fn seed_pending(&mut self, names: &[&str], day: i32, include: &[Option<bool>]) {
        let clocks = names
            .iter()
            .enumerate()
            .map(|(i, _)| i64::from(day) * KNOWN_DAY_NS + KNOWN_DAY_NS / 2 + i as i64 * 1_000_000)
            .collect::<Vec<_>>();
        let input = RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(clocks.clone())),
                Arc::new(BooleanArray::from(include.to_vec())),
                Arc::new(TimestampNanosecondArray::from(clocks)),
            ],
        )
        .unwrap();
        let mut out = AggregateCollector::default();
        self.operator
            .process_batch(input, &mut self.ctx, &mut out)
            .await
            .unwrap();
        assert!(out.batches.is_empty());
    }

    fn group(&self, name: &str) -> Vec<u8> {
        self.operator
            .key_converter
            .convert_columns(&[Arc::new(StringArray::from(vec![name]))])
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec()
    }

    async fn finish(self) {
        let Self {
            operator,
            ctx,
            backend,
            resources,
            trace,
            root,
        } = self;
        drop(operator);
        drop(ctx);
        assert_eq!(trace.snapshots.load(Ordering::SeqCst), 0);
        assert!(
            resources
                .try_decoded_value(resources.config().decoded_value_bytes)
                .is_ok()
        );
        assert!(
            resources
                .try_queued_write(resources.config().queued_write_bytes)
                .is_ok()
        );
        tokio::time::timeout(Duration::from_secs(10), backend.close())
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn known_watermark(day: i32) -> Watermark {
    calendar_dispatch_watermark(i64::from(day))
}

fn known_rows(out: &AggregateCollector) -> Vec<(String, bool, [i64; 5], i64)> {
    let mut rows = Vec::new();
    for batch in &out.batches {
        let key = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let retract = IncrementalAggregatingFunc::get_retracts(batch).unwrap();
        let clock = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                key.value(row).to_owned(),
                retract.value(row),
                std::array::from_fn(|i| {
                    batch
                        .column(i + 1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                }),
                clock.value(row),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn calendar_known_owner_order_filters_stale_and_tight_fallback() {
    for rocks in [false, true] {
        let mut reference = None;
        for fallback in [false, true] {
            let mut f = KnownFixture::new(rocks, fallback).await;
            let names = ["q0000", "q0001", "q0002", "q0003"];
            f.seed(
                &names,
                KNOWN_DAY,
                &[Some(true), Some(false), Some(true), None],
            )
            .await;
            f.seed(
                &names,
                KNOWN_DAY + 1,
                &[Some(false), Some(true), Some(true), Some(true)],
            )
            .await;
            let group = f.group(names[0]);
            {
                let mut scope = f
                    .operator
                    .native_store
                    .as_ref()
                    .unwrap()
                    .begin()
                    .await
                    .unwrap();
                // An earlier, generation-mismatched H must not publish G.
                scope
                    .put(
                        &calendar_native::calendar_due_key(
                            i64::from(KNOWN_DAY) * KNOWN_DAY_NS,
                            &group,
                        )
                        .unwrap(),
                        &999_u64.to_be_bytes(),
                    )
                    .unwrap();
                scope.commit().await.unwrap();
            }
            f.trace.dirty_gets.store(0, Ordering::SeqCst);
            f.trace.dirty_scans.store(0, Ordering::SeqCst);
            let mut out = AggregateCollector::default();
            f.operator
                .handle_watermark(known_watermark(KNOWN_DAY + 2), &mut f.ctx, &mut out)
                .await
                .unwrap();
            assert_eq!(
                f.trace.dirty_gets.load(Ordering::SeqCst),
                if fallback { 0 } else { 5 }
            );
            assert!(f.trace.dirty_scans.load(Ordering::SeqCst) >= 1);
            if !fallback {
                assert_eq!(f.trace.dirty_scans.load(Ordering::SeqCst), 1);
            }
            let actual = known_rows(&out);
            let mut expected = Vec::new();
            for (i, recent) in [(1, 1), (2, 2), (3, 1)] {
                let clock = i64::from(KNOWN_DAY + 1) * KNOWN_DAY_NS
                    + KNOWN_DAY_NS / 2
                    + i as i64 * 1_000_000;
                expected.push((
                    names[i].to_owned(),
                    true,
                    [2, 1, recent, recent, recent],
                    clock,
                ));
                expected.push((
                    names[i].to_owned(),
                    false,
                    [2, 0, recent, recent, recent],
                    clock,
                ));
            }
            assert_eq!(actual, expected, "quiet/suppressed/filter/order oracle");
            if let Some(reference) = &reference {
                assert_eq!(&actual, reference);
            } else {
                reference = Some(actual);
            }
            // Leave the earlier key DIRTY: only the next watermark's initial
            // full drain may publish its append before the quiet replacement.
            f.seed_pending(&["p0000"], KNOWN_DAY + 2, &[Some(true)])
                .await;
            let earlier = f.group("p0000");
            {
                let scope = f
                    .operator
                    .native_store
                    .as_ref()
                    .unwrap()
                    .begin_point()
                    .await
                    .unwrap();
                assert!(
                    scope
                        .get(&native_group_key(b'D', &earlier).unwrap())
                        .await
                        .unwrap()
                        .is_some(),
                    "earlier D must be pending before callback"
                );
            }
            let mut next = AggregateCollector::default();
            f.operator
                .handle_watermark(known_watermark(KNOWN_DAY + 3), &mut f.ctx, &mut next)
                .await
                .unwrap();
            let next = known_rows(&next);
            let clock = i64::from(KNOWN_DAY + 2) * KNOWN_DAY_NS + KNOWN_DAY_NS / 2;
            assert_eq!(
                next,
                vec![
                    ("p0000".into(), false, [1; 5], clock),
                    ("p0000".into(), true, [1; 5], clock),
                    ("p0000".into(), false, [1, 0, 1, 1, 1], clock),
                ],
                "initial cold drain must precede next quiet-owner replacement"
            );
            f.finish().await;
        }
    }
}

struct KnownSlowCollector {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[async_trait::async_trait]
impl Collector for KnownSlowCollector {
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        assert_eq!(batch.num_rows(), 2, "expected rollover replacement");
        self.entered.take().unwrap().send(()).unwrap();
        (&mut self.release)
            .await
            .map_err(|_| anyhow!("test collector cancelled"))?;
        drop(batch);
        Ok(())
    }
    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn calendar_known_owner_slow_collection_cancel_and_completion_release() {
    for rocks in [false, true] {
        for cancel in [false, true] {
            let mut f = KnownFixture::new(rocks, false).await;
            f.seed(&["q0000"], KNOWN_DAY + 1, &[Some(true)]).await;
            let group = f.group("q0000");
            let native = f.operator.native_config.unwrap();
            let (entered, mut ready) = tokio::sync::oneshot::channel();
            let (release, blocked) = tokio::sync::oneshot::channel();
            let mut out = KnownSlowCollector {
                entered: Some(entered),
                release: blocked,
            };
            let mut future = Box::pin(f.operator.handle_watermark(
                known_watermark(KNOWN_DAY + 2),
                &mut f.ctx,
                &mut out,
            ));
            tokio::select! {
                value = &mut future => panic!("collector did not suspend: {value:?}"),
                value = &mut ready => value.unwrap(),
                _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("collector gate timeout"),
            }
            assert_eq!(f.trace.dirty_gets.load(Ordering::SeqCst), 1);
            assert_eq!(
                f.trace.snapshots.load(Ordering::SeqCst),
                0,
                "snapshot retained through collect"
            );
            assert!(
                f.resources
                    .try_queued_write(f.resources.config().queued_write_bytes)
                    .is_ok()
            );
            let progress =
                native.key_bytes * 4 + std::mem::size_of::<AggregateExpiryProgress>() + 128;
            let free = f.resources.config().decoded_value_bytes
                - native.max_pending_output_bytes * 3
                - progress;
            assert!(f.resources.try_decoded_value(free).is_ok());
            assert!(f.resources.try_decoded_value(free + 1).is_err());
            if cancel {
                drop(future);
                drop(out);
                assert!(release.send(()).is_err());
            } else {
                release.send(()).unwrap();
                future.await.unwrap();
            }
            assert!(
                f.resources
                    .try_decoded_value(f.resources.config().decoded_value_bytes)
                    .is_ok()
            );
            {
                let scope = f
                    .operator
                    .native_store
                    .as_ref()
                    .unwrap()
                    .begin_point()
                    .await
                    .unwrap();
                assert!(
                    scope
                        .get(&native_group_key(b'D', &group).unwrap())
                        .await
                        .unwrap()
                        .is_none()
                );
                let bytes = scope
                    .get(&native_group_key(b'G', &group).unwrap())
                    .await
                    .unwrap()
                    .unwrap();
                let current = f
                    .operator
                    .native_group_codec()
                    .unwrap()
                    .decode(&bytes, scope.limits().value_bytes)
                    .unwrap();
                assert!(current.last_emitted.is_some());
            }
            let mut retry = AggregateCollector::default();
            f.operator
                .handle_watermark(known_watermark(KNOWN_DAY + 2), &mut f.ctx, &mut retry)
                .await
                .unwrap();
            assert!(
                retry.batches.is_empty(),
                "post-output-commit cancellation duplicated CDC"
            );
            f.finish().await;
        }
    }
}

#[tokio::test]
async fn calendar_known_owner_cancel_after_dispatch_keeps_pending_d_for_full_drain() {
    for rocks in [false, true] {
        let mut f = KnownFixture::new(rocks, false).await;
        f.seed(&["q0000"], KNOWN_DAY + 1, &[Some(true)]).await;
        let group = f.group("q0000");
        f.trace.suspend_dirty_get.store(true, Ordering::SeqCst);
        let mut out = AggregateCollector::default();
        let mut future = Box::pin(f.operator.handle_watermark(
            known_watermark(KNOWN_DAY + 2),
            &mut f.ctx,
            &mut out,
        ));
        tokio::select! {
            value = &mut future => panic!("known D read did not suspend: {value:?}"),
            _ = async { while f.trace.suspended.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {},
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("known D gate timeout"),
        }
        drop(future);
        assert!(out.batches.is_empty());
        assert_eq!(f.trace.snapshots.load(Ordering::SeqCst), 0);
        assert!(
            f.resources
                .try_decoded_value(f.resources.config().decoded_value_bytes)
                .is_ok()
        );
        {
            let scope = f
                .operator
                .native_store
                .as_ref()
                .unwrap()
                .begin_point()
                .await
                .unwrap();
            assert!(
                scope
                    .get(&native_group_key(b'D', &group).unwrap())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        f.trace.dirty_gets.store(0, Ordering::SeqCst);
        let mut retry = AggregateCollector::default();
        f.operator
            .handle_watermark(known_watermark(KNOWN_DAY + 2), &mut f.ctx, &mut retry)
            .await
            .unwrap();
        let clock = i64::from(KNOWN_DAY + 1) * KNOWN_DAY_NS + KNOWN_DAY_NS / 2;
        assert_eq!(
            known_rows(&retry),
            vec![
                ("q0000".into(), true, [1; 5], clock),
                ("q0000".into(), false, [1, 0, 1, 1, 1], clock),
            ]
        );
        assert_eq!(
            f.trace.dirty_gets.load(Ordering::SeqCst),
            0,
            "retry must use initial full D drain"
        );
        f.finish().await;
    }
}
