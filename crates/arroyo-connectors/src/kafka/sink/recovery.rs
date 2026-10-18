//! Compact prepared-transaction recovery with an atomic Kafka marker.
use super::super::{Context, FutureProducer};
use anyhow::{Result, anyhow, ensure};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

pub const PROTOCOL_VERSION: u32 = 4;
pub const MAX_METADATA_BYTES: usize = 4096;
pub const DEFAULT_QUEUE_KBYTES: usize = 8192;
pub const MAX_DELIVERY_FUTURES: usize = 1024;

pub fn require(condition: bool, message: &str) -> Result<()> {
    ensure!(condition, "{message}");
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryState {
    pub version: u32,
    pub generation: String,
    pub next_transaction_index: usize,
    pub checkpoint_epoch: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingCommit {
    pub version: u32,
    pub generation: String,
    pub epoch: u32,
    pub transaction_index: usize,
    pub producer_id: i64,
    pub producer_epoch: i16,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    version: u32,
    generation: String,
    epoch: u32,
}

pub fn decode_state(bytes: &[u8]) -> Result<RecoveryState> {
    ensure!(
        bytes.len() <= MAX_METADATA_BYTES,
        "Kafka recovery state exceeds bounded size"
    );
    let state: RecoveryState = serde_json::from_slice(bytes)?;
    ensure!(
        state.version == PROTOCOL_VERSION
            && !state.generation.is_empty()
            && state.next_transaction_index > 0
            && state.next_transaction_index < usize::MAX,
        "Invalid Kafka recovery state"
    );
    Ok(state)
}
pub fn decode_pending(bytes: &[u8], epoch: u32) -> Result<PendingCommit> {
    ensure!(
        bytes.len() <= MAX_METADATA_BYTES,
        "Kafka transaction metadata exceeds bounded size"
    );
    let pending: PendingCommit = serde_json::from_slice(bytes)?;
    ensure!(
        pending.version == PROTOCOL_VERSION
            && pending.epoch == epoch
            && !pending.generation.is_empty()
            && pending.producer_id >= 0
            && pending.producer_epoch >= 0,
        "Invalid Kafka prepared transaction identity"
    );
    Ok(pending)
}
pub fn encode_pending(pending: &PendingCommit) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(pending)?;
    decode_pending(&bytes, pending.epoch)?;
    Ok(bytes)
}
pub fn validate_marker_epoch(marker_epoch: u32, checkpoint_epoch: u32) -> Result<()> {
    ensure!(
        marker_epoch <= checkpoint_epoch,
        "Kafka marker is newer than restored checkpoint; refusing rollback that could duplicate committed output"
    );
    Ok(())
}

fn native_result(error: *mut rdkafka::bindings::rd_kafka_error_t) -> Result<()> {
    if error.is_null() {
        return Ok(());
    }
    // SAFETY: the native API transfers one owned error, whose string remains
    // valid until destruction. No private native layout is accessed.
    unsafe {
        let message = std::ffi::CStr::from_ptr(rdkafka::bindings::rd_kafka_error_string(error))
            .to_string_lossy()
            .into_owned();
        rdkafka::bindings::rd_kafka_error_destroy(error);
        Err(anyhow!("Kafka prepared transaction API: {message}"))
    }
}
pub fn prepare(producer: &FutureProducer) -> Result<(i64, i16)> {
    let mut id = -1;
    let mut epoch = -1;
    // SAFETY: producer owns the live native handle; output references are valid
    // for the synchronous API call, which seals and flushes before exporting.
    native_result(unsafe {
        rdkafka::bindings::rd_kafka_streamr_prepare_transaction(
            producer.client().native_ptr(),
            &mut id,
            &mut epoch,
            10_000,
        )
    })?;
    Ok((id, epoch))
}
pub fn resume(producer: &FutureProducer, pending: &PendingCommit) -> Result<()> {
    // SAFETY: this API accepts only a fresh uninitialized native producer and
    // installs a commit-only identity under the native main-thread lock.
    native_result(unsafe {
        rdkafka::bindings::rd_kafka_streamr_resume_commit(
            producer.client().native_ptr(),
            pending.producer_id,
            pending.producer_epoch,
        )
    })
}
pub fn retry_transaction(
    mut op: impl FnMut() -> std::result::Result<(), KafkaError>,
) -> Result<()> {
    for attempt in 0..6 {
        match op() {
            Ok(()) => return Ok(()),
            Err(KafkaError::Transaction(e)) if e.is_retriable() && attempt < 5 => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                return Err(anyhow!(
                    "Kafka transaction failed without acknowledgement: {e}"
                ));
            }
        }
    }
    unreachable!()
}
pub async fn validate_topic(config: &ClientConfig, context: Context, topic: &str) -> Result<()> {
    let admin: AdminClient<Context> = config.create_with_context(context)?;
    let metadata = admin
        .inner()
        .fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let metadata_topic = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("Kafka recovery topic missing"))?;
    ensure!(
        metadata_topic.error().is_none() && metadata_topic.partitions().len() == 1,
        "Kafka recovery topic must exist with exactly one partition"
    );
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let result = admin
        .describe_configs([&ResourceSpecifier::Topic(topic)], &opts)
        .await?;
    let resource = result
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Missing Kafka recovery topic config"))?
        .map_err(|e| anyhow!("Cannot inspect preexisting Kafka recovery topic: {e:?}"))?;
    let policy = resource
        .get("cleanup.policy")
        .and_then(|e| e.value.as_deref());
    ensure!(
        policy == Some("compact"),
        "sink.recovery_topic must be preexisting with cleanup.policy=compact only"
    );
    Ok(())
}
pub async fn send_marker(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    generation: &str,
    epoch: u32,
) -> Result<()> {
    let payload = serde_json::to_vec(&Marker {
        version: PROTOCOL_VERSION,
        generation: generation.into(),
        epoch,
    })?;
    producer
        .send(
            FutureRecord::to(topic)
                .key(key)
                .payload(&payload)
                .partition(0),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| anyhow!("Kafka commit marker delivery failed: {e}"))?;
    Ok(())
}
pub async fn scan_marker(
    config: &ClientConfig,
    context: Context,
    topic: &str,
    key: &str,
    generation: &str,
) -> Result<u32> {
    let mut config = config.clone();
    config
        .set(
            "group.id",
            format!("streamr-recovery-{}", uuid::Uuid::now_v7()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .set("allow.auto.create.topics", "false");
    let consumer: BaseConsumer<Context> = config.create_with_context(context)?;
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let t = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("Kafka recovery topic missing"))?;
    ensure!(
        t.error().is_none() && t.partitions().len() == 1,
        "Kafka recovery topic must exist and have exactly one partition"
    );
    let (low, high) = consumer.fetch_watermarks(topic, 0, Duration::from_secs(10))?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, 0, Offset::Offset(low))?;
    consumer.assign(&assignment)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut latest = None;
    let mut done = low == high;
    while !done && Instant::now() < deadline {
        match consumer.poll(Duration::ZERO) {
            Some(Ok(msg)) => {
                if msg.offset() < high && msg.key() == Some(key.as_bytes()) {
                    let payload = msg
                        .payload()
                        .ok_or_else(|| anyhow!("Kafka recovery marker was deleted"))?;
                    ensure!(
                        payload.len() <= 4096,
                        "Kafka recovery marker exceeds bounded size"
                    );
                    latest = Some(serde_json::from_slice::<Marker>(payload)?);
                }
                done = msg.offset() >= high - 1;
            }
            Some(Err(KafkaError::PartitionEOF(_))) | None => {
                let position = consumer.position()?;
                done = position
                    .find_partition(topic, 0)
                    .map(|p| matches!(p.offset(), Offset::Offset(n) if n >= high))
                    .unwrap_or(false);
                if !done {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Some(Err(e)) => return Err(e.into()),
        }
    }
    ensure!(
        done,
        "Kafka recovery marker scan could not reach captured high watermark; refusing ambiguous replay"
    );
    let marker = latest.ok_or_else(|| {
        anyhow!(
            "Kafka recovery generation sentinel missing; refusing replay after possible topic loss"
        )
    })?;
    ensure!(
        marker.version == PROTOCOL_VERSION && marker.generation == generation,
        "Kafka recovery marker generation mismatch; topic may have been replaced"
    );
    Ok(marker.epoch)
}
pub async fn fault_pause(job_id: &str, epoch: u32, point: &str, nonempty: bool) {
    if !nonempty {
        return;
    }
    let Ok(dir) = std::env::var("STREAMR_TEST_KAFKA_COMMIT_FAULT_DIR") else {
        return;
    };
    let path = std::path::Path::new(&dir).join(format!("{point}-{job_id}"));
    if path.exists() {
        tracing::warn!(job_id, epoch, point, "Kafka commit fault pause");
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn pending() -> PendingCommit {
        PendingCommit {
            version: PROTOCOL_VERSION,
            generation: "g".into(),
            epoch: 9,
            transaction_index: 4,
            producer_id: 123,
            producer_epoch: 2,
        }
    }
    #[test]
    fn compact_transaction_metadata_validates_identity() {
        let p = pending();
        let bytes = encode_pending(&p).unwrap();
        assert!(bytes.len() < 256);
        assert_eq!(decode_pending(&bytes, 9).unwrap().producer_id, 123);
        assert!(decode_pending(&bytes, 8).is_err());
        assert!(
            encode_pending(&PendingCommit {
                producer_id: -1,
                ..p
            })
            .is_err()
        );
        assert!(decode_pending(&vec![0; MAX_METADATA_BYTES + 1], 9).is_err());
        assert!(decode_pending(br#"{"version":2,"records":[]}"#, 9).is_err());
    }
    #[test]
    fn rollback_is_rejected() {
        assert!(validate_marker_epoch(10, 9).is_err());
        assert!(validate_marker_epoch(9, 9).is_ok());
        assert!(validate_marker_epoch(8, 9).is_ok());
    }
    #[test]
    fn state_requires_generation_and_index() {
        let state = RecoveryState {
            version: PROTOCOL_VERSION,
            generation: "g".into(),
            next_transaction_index: 2,
            checkpoint_epoch: 4,
        };
        assert!(decode_state(&serde_json::to_vec(&state).unwrap()).is_ok());
        assert!(
            decode_state(
                &serde_json::to_vec(&RecoveryState {
                    next_transaction_index: 0,
                    ..state
                })
                .unwrap()
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn native_resume_rejects_invalid_identity_and_reuse_without_broker() {
        let producer: FutureProducer = ClientConfig::new()
            .set("transactional.id", "native-resume-unit")
            .create_with_context(Context::new(None))
            .unwrap();
        assert!(prepare(&producer).is_err());
        let nontransactional: FutureProducer = ClientConfig::new()
            .create_with_context(Context::new(None))
            .unwrap();
        assert!(resume(&nontransactional, &pending()).is_err());
        assert!(
            resume(
                &producer,
                &PendingCommit {
                    producer_epoch: -1,
                    ..pending()
                }
            )
            .is_err()
        );
        assert!(
            resume(
                &producer,
                &PendingCommit {
                    producer_id: -1,
                    ..pending()
                }
            )
            .is_err()
        );
        resume(&producer, &pending()).unwrap();
        assert!(resume(&producer, &pending()).is_err());
        assert!(producer.begin_transaction().is_err());
        assert!(
            producer
                .init_transactions(Duration::from_millis(10))
                .is_err()
        );
        assert!(
            producer
                .abort_transaction(Duration::from_millis(10))
                .is_err()
        );
        let send =
            producer.send_result(FutureRecord::<str, str>::to("test").payload("must-not-send"));
        assert!(send.is_err());
    }
    #[tokio::test]
    async fn native_prepared_resume_remains_commit_only_after_abortable_error() {
        use rdkafka::mocking::MockCluster;
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("prepared-contract", 1, 1).unwrap();
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("transactional.id", "prepared-contract-transaction");
        let original: FutureProducer = config.create_with_context(Context::new(None)).unwrap();
        original.init_transactions(Duration::from_secs(5)).unwrap();
        original.begin_transaction().unwrap();
        original
            .send(
                FutureRecord::<str, str>::to("prepared-contract")
                    .payload("durable-before-prepare")
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let (producer_id, producer_epoch) = prepare(&original).unwrap();
        assert!(
            original
                .send_result(
                    FutureRecord::<str, str>::to("prepared-contract")
                        .payload("forbidden-after-prepare")
                )
                .is_err()
        );
        let recovered: FutureProducer = config.create_with_context(Context::new(None)).unwrap();
        resume(
            &recovered,
            &PendingCommit {
                producer_id,
                producer_epoch,
                ..pending()
            },
        )
        .unwrap();
        cluster.request_errors(
            RDKafkaApiKey::EndTxn,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID],
        );
        // Resume installs identity locally; native coordinator discovery can
        // still be in progress. Exercise the same bounded retry policy used by
        // recovery, then require the injected non-retriable broker error.
        let mut attempts = Vec::new();
        let result = retry_transaction(|| {
            let result = recovered.commit_transaction(Duration::from_secs(5));
            if let Err(error) = &result {
                attempts.push(error.clone());
            }
            result
        });
        eprintln!("prepared commit native error attempts: {attempts:?}");
        assert!(result.is_err(), "injected EndTxn error must be observed");
        let error = attempts.last().expect("native commit must fail");
        assert_eq!(
            error.rdkafka_error_code(),
            Some(rdkafka::error::RDKafkaErrorCode::UnknownProducerId),
            "expected injected EndTxn error, got {attempts:?}"
        );
        assert!(
            matches!(error, KafkaError::Transaction(e) if e.txn_requires_abort()),
            "expected abortable error after coordinator discovery, got {attempts:?}"
        );
        let abort = recovered
            .abort_transaction(Duration::from_secs(5))
            .unwrap_err();
        assert_eq!(
            abort.rdkafka_error_code(),
            Some(rdkafka::error::RDKafkaErrorCode::State)
        );
        assert!(recovered.begin_transaction().is_err());
        assert!(
            recovered
                .send_result(
                    FutureRecord::<str, str>::to("prepared-contract")
                        .payload("forbidden-after-error")
                )
                .is_err()
        );
    }
    #[tokio::test]
    async fn native_prepared_resume_waits_for_coordinator_with_existing_api_timeout() {
        use rdkafka::mocking::MockCluster;
        use std::time::Instant;

        // Each fresh client must discover and connect its own coordinator.
        // Mock broker latency exceeds the former six 100 ms caller retries.
        for scenario in ["single-call", "timeout-resume", "destroy-waiting"] {
            let cluster = MockCluster::new(1).unwrap();
            cluster.create_topic("delayed-prepared", 1, 1).unwrap();
            let mut config = ClientConfig::new();
            config
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .set("transactional.id", format!("delayed-{scenario}"));
            let original: FutureProducer = config.create_with_context(Context::new(None)).unwrap();
            original.init_transactions(Duration::from_secs(5)).unwrap();
            original.begin_transaction().unwrap();
            original
                .send(
                    FutureRecord::<str, str>::to("delayed-prepared")
                        .payload("prepared-before-delay")
                        .partition(0),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
            let (producer_id, producer_epoch) = prepare(&original).unwrap();
            cluster
                .broker_round_trip_time(1, Duration::from_millis(1000))
                .unwrap();
            let recovered: FutureProducer = config.create_with_context(Context::new(None)).unwrap();
            resume(
                &recovered,
                &PendingCommit {
                    producer_id,
                    producer_epoch,
                    ..pending()
                },
            )
            .unwrap();
            let started = Instant::now();
            if scenario == "single-call" {
                // No application retries: native readiness must honor this call.
                recovered
                    .commit_transaction(Duration::from_secs(10))
                    .unwrap();
                assert!(started.elapsed() >= Duration::from_millis(1000));
            } else {
                let error = recovered
                    .commit_transaction(Duration::from_millis(50))
                    .unwrap_err();
                assert_eq!(
                    error.rdkafka_error_code(),
                    Some(rdkafka::error::RDKafkaErrorCode::OperationTimedOut),
                    "{scenario}: {error:?}"
                );
                assert!(matches!(error, KafkaError::Transaction(ref e) if e.is_retriable()));
                assert!(recovered.begin_transaction().is_err());
                if scenario == "timeout-resume" {
                    recovered
                        .commit_transaction(Duration::from_secs(10))
                        .unwrap();
                } else {
                    // Pending readiness owns no extra reference or detached task.
                    let dropping = Instant::now();
                    drop(recovered);
                    assert!(dropping.elapsed() < Duration::from_secs(5));
                }
            }
        }
    }
}
