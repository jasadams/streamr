//! Bounded checkpoint replay with an atomic Kafka commit marker.
use super::super::{Context, FutureProducer};
use anyhow::{Result, anyhow, ensure};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub fn require(condition: bool, message: &str) -> Result<()> {
    ensure!(condition, "{message}");
    Ok(())
}

pub const PROTOCOL_VERSION: u32 = 2;
pub const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;
// CommitRequest uses tonic's bounded message transport. Keep a single-subtask
// journal comfortably below its 4 MiB frame; aggregate sizing is a separate gate.
pub const MAX_COMMIT_METADATA_BYTES: usize = 1024 * 1024;
const REFERENCE_VERSION: u32 = 3;
const MAX_REFERENCE_BYTES: usize = 4096;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplayRecord {
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub partition: i32,
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
    pub records: Vec<ReplayRecord>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalReference {
    version: u32,
    generation: String,
    epoch: u32,
    transaction_index: usize,
    journal_bytes: u64,
    journal_sha256: [u8; 32],
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
        bytes.len() <= 4096,
        "Kafka recovery state exceeds bounded size"
    );
    let state: RecoveryState = serde_json::from_slice(bytes)
        .map_err(|e| anyhow!("Unsupported legacy/corrupt Kafka checkpoint: {e}"))?;
    ensure!(
        state.version == PROTOCOL_VERSION
            && !state.generation.is_empty()
            && state.next_transaction_index > 0
            && state.next_transaction_index < usize::MAX,
        "Invalid Kafka recovery state"
    );
    Ok(state)
}

pub fn validate_marker_epoch(marker_epoch: u32, checkpoint_epoch: u32) -> Result<()> {
    ensure!(
        marker_epoch <= checkpoint_epoch,
        "Kafka marker is newer than restored checkpoint; refusing rollback that could duplicate committed output"
    );
    Ok(())
}

pub fn check_budget(used: usize, record: &ReplayRecord, max: usize) -> Result<usize> {
    check_record_budget(
        used,
        record.payload.len(),
        record.key.as_ref().map_or(0, Vec::len),
        max,
    )
}
pub fn check_record_budget(
    used: usize,
    payload_bytes: usize,
    key_bytes: usize,
    max: usize,
) -> Result<usize> {
    let bytes = payload_bytes
        .checked_add(key_bytes)
        .and_then(|n| n.checked_add(64))
        .ok_or_else(|| anyhow!("Kafka replay journal size overflow"))?;
    let next = used
        .checked_add(bytes)
        .ok_or_else(|| anyhow!("Kafka replay journal size overflow"))?;
    ensure!(
        next <= max,
        "Kafka checkpoint replay journal exceeds sink.recovery_max_bytes ({max}); reduce checkpoint interval or increase the explicit bounded budget"
    );
    Ok(next)
}
pub fn decode_pending(bytes: &[u8], epoch: u32, max: usize) -> Result<PendingCommit> {
    // JSON byte arrays can expand by up to four times; bound before deserialization.
    ensure!(
        bytes.len() <= MAX_COMMIT_METADATA_BYTES
            && bytes.len() <= max.saturating_mul(5).saturating_add(4096),
        "Kafka replay metadata exceeds configured budget"
    );
    decode_journal(bytes, epoch, max)
}

fn encoded_journal_limit(max: usize) -> usize {
    max.saturating_mul(5).saturating_add(4096)
}

fn validate_pending(pending: &PendingCommit, epoch: u32, max: usize) -> Result<()> {
    ensure!(
        pending.version == PROTOCOL_VERSION
            && pending.epoch == epoch
            && !pending.generation.is_empty(),
        "Kafka replay checkpoint identity/version mismatch"
    );
    let mut used = 0;
    for record in &pending.records {
        ensure!(record.partition >= 0, "Kafka replay partition missing");
        used = check_budget(used, record, max)?;
    }
    Ok(())
}

fn decode_journal(bytes: &[u8], epoch: u32, max: usize) -> Result<PendingCommit> {
    // Preserve the configured raw budget while bounding JSON byte-array expansion
    // before deserialization. Journals live in checkpoint files, not RPC metadata.
    ensure!(
        bytes.len() <= encoded_journal_limit(max),
        "Kafka replay journal exceeds configured encoded budget"
    );
    let pending: PendingCommit = serde_json::from_slice(bytes)
        .map_err(|e| anyhow!("Unsupported legacy/corrupt Kafka committing checkpoint: {e}"))?;
    validate_pending(&pending, epoch, max)?;
    Ok(pending)
}

/// All new checkpoints persist the journal in an ordinary checkpoint table and
/// send only this small reference through controller commit RPCs.
pub fn encode_pending(pending: &PendingCommit, max: usize) -> Result<(Vec<u8>, Vec<u8>)> {
    validate_pending(pending, pending.epoch, max)?;
    let journal = serde_json::to_vec(pending)?;
    ensure!(
        journal.len() <= encoded_journal_limit(max),
        "Kafka replay journal exceeds configured encoded budget"
    );
    let reference = serde_json::to_vec(&JournalReference {
        version: REFERENCE_VERSION,
        generation: pending.generation.clone(),
        epoch: pending.epoch,
        transaction_index: pending.transaction_index,
        journal_bytes: u64::try_from(journal.len())?,
        journal_sha256: Sha256::digest(&journal).into(),
    })?;
    ensure!(
        reference.len() <= MAX_REFERENCE_BYTES,
        "Kafka journal reference exceeds bounded size"
    );
    Ok((journal, reference))
}

pub fn decode_commit(
    bytes: &[u8],
    journal: Option<&[u8]>,
    epoch: u32,
    max: usize,
) -> Result<PendingCommit> {
    ensure!(
        bytes.len() <= MAX_COMMIT_METADATA_BYTES,
        "Kafka commit metadata exceeds bounded size"
    );
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    let version: Version = serde_json::from_slice(bytes)?;
    if version.version == PROTOCOL_VERSION {
        // Read deployed v2 in-flight checkpoints; new writes always use references.
        return decode_pending(bytes, epoch, max);
    }
    ensure!(
        version.version == REFERENCE_VERSION && bytes.len() <= MAX_REFERENCE_BYTES,
        "Unsupported/oversized Kafka journal reference"
    );
    let reference: JournalReference = serde_json::from_slice(bytes)?;
    ensure!(
        reference.epoch == epoch && !reference.generation.is_empty(),
        "Kafka journal reference identity mismatch"
    );
    let journal =
        journal.ok_or_else(|| anyhow!("Selected checkpoint is missing Kafka replay journal"))?;
    ensure!(
        journal.len() <= encoded_journal_limit(max),
        "Kafka replay journal exceeds configured encoded budget"
    );
    ensure!(
        u64::try_from(journal.len())? == reference.journal_bytes,
        "Kafka replay journal length mismatch"
    );
    ensure!(
        <[u8; 32]>::from(Sha256::digest(journal)) == reference.journal_sha256,
        "Kafka replay journal hash mismatch"
    );
    let pending = decode_journal(journal, epoch, max)?;
    ensure!(
        pending.generation == reference.generation
            && pending.transaction_index == reference.transaction_index,
        "Kafka replay journal/reference identity mismatch"
    );
    Ok(pending)
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
pub async fn replay(producer: &FutureProducer, topic: &str, pending: &PendingCommit) -> Result<()> {
    producer.begin_transaction()?;
    for r in &pending.records {
        let mut record = FutureRecord::<Vec<u8>, Vec<u8>>::to(topic)
            .payload(&r.payload)
            .partition(r.partition);
        if let Some(key) = &r.key {
            record = record.key(key);
        }
        if let Some(ts) = r.timestamp {
            record = record.timestamp(ts);
        }
        producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| anyhow!("Kafka replay delivery failed: {e}"))?;
    }
    Ok(())
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
    fn record() -> ReplayRecord {
        ReplayRecord {
            timestamp: Some(123),
            key: Some(vec![0, 255]),
            payload: vec![1, 2, 3],
            partition: 2,
        }
    }
    #[test]
    fn budget_rejects_before_publish() {
        assert_eq!(check_budget(0, &record(), 69).unwrap(), 69);
        assert!(check_budget(1, &record(), 69).is_err());
        assert!(check_budget(usize::MAX, &record(), usize::MAX).is_err());
        assert!(check_record_budget(0, usize::MAX, 0, usize::MAX).is_err());
    }
    #[test]
    fn replay_preserves_wire_record() {
        let p = PendingCommit {
            version: PROTOCOL_VERSION,
            generation: "g".into(),
            epoch: 9,
            transaction_index: 4,
            records: vec![record()],
        };
        let bytes = serde_json::to_vec(&p).unwrap();
        let decoded = decode_pending(&bytes, 9, 100).unwrap();
        assert_eq!(decoded.records, p.records);
        assert!(decode_pending(&bytes, 8, 100).is_err());
        assert!(decode_pending(&bytes, 9, 68).is_err());
    }
    #[test]
    fn state_requires_generation_and_epoch() {
        let state = RecoveryState {
            version: PROTOCOL_VERSION,
            generation: "g".into(),
            next_transaction_index: 2,
            checkpoint_epoch: 4,
        };
        assert_eq!(
            decode_state(&serde_json::to_vec(&state).unwrap())
                .unwrap()
                .checkpoint_epoch,
            4
        );
        assert!(
            decode_state(br#"{"version":1,"generation":"g","next_transaction_index":2}"#).is_err()
        );
        assert!(decode_state(b"2").is_err());
        let empty = RecoveryState {
            generation: String::new(),
            ..state
        };
        assert!(decode_state(&serde_json::to_vec(&empty).unwrap()).is_err());
    }
    #[test]
    fn terminal_transaction_error_is_not_retried() {
        let mut calls = 0;
        assert!(
            retry_transaction(|| {
                calls += 1;
                Err(KafkaError::ClientCreation("fatal".into()))
            })
            .is_err()
        );
        assert_eq!(calls, 1);
    }
    #[test]
    fn rollback_is_rejected() {
        assert!(validate_marker_epoch(10, 9).is_err());
        assert!(validate_marker_epoch(9, 9).is_ok());
        assert!(validate_marker_epoch(8, 9).is_ok());
    }
    #[test]
    fn legacy_checkpoint_fails_closed() {
        assert!(decode_pending(&[1, 2, 3], 1, 100).is_err());
    }
    #[test]
    fn large_journal_uses_small_checkpoint_commit_reference() {
        let pending = PendingCommit {
            version: PROTOCOL_VERSION,
            generation: "g".into(),
            epoch: 1,
            transaction_index: 1,
            records: vec![ReplayRecord {
                payload: vec![255; MAX_COMMIT_METADATA_BYTES / 4],
                ..record()
            }],
        };
        let (journal, reference) = encode_pending(&pending, DEFAULT_MAX_BYTES).unwrap();
        assert!(journal.len() > MAX_COMMIT_METADATA_BYTES);
        assert!(reference.len() < MAX_REFERENCE_BYTES);
        assert_eq!(
            decode_commit(&reference, Some(&journal), 1, DEFAULT_MAX_BYTES)
                .unwrap()
                .records,
            pending.records
        );
        assert!(decode_commit(&reference, None, 1, DEFAULT_MAX_BYTES).is_err());
        let mut corrupted = journal.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(decode_commit(&reference, Some(&corrupted), 1, DEFAULT_MAX_BYTES).is_err());
        assert!(decode_commit(&reference, Some(&journal), 2, DEFAULT_MAX_BYTES).is_err());
        assert!(decode_commit(&reference, Some(&journal), 1, 100).is_err());
        assert!(
            decode_pending(
                &vec![0; MAX_COMMIT_METADATA_BYTES + 1],
                1,
                DEFAULT_MAX_BYTES
            )
            .is_err()
        );
    }
    #[test]
    fn journal_reference_rejects_wrong_length_generation_and_transaction() {
        let pending = PendingCommit {
            version: PROTOCOL_VERSION,
            generation: "g".into(),
            epoch: 9,
            transaction_index: 4,
            records: vec![record()],
        };
        let (journal, bytes) = encode_pending(&pending, 100).unwrap();
        for field in ["journal_bytes", "generation", "transaction_index"] {
            let mut reference: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            reference[field] = match field {
                "generation" => "other".into(),
                _ => 999.into(),
            };
            assert!(
                decode_commit(
                    &serde_json::to_vec(&reference).unwrap(),
                    Some(&journal),
                    9,
                    100
                )
                .is_err()
            );
        }
        assert!(encode_pending(&pending, 68).is_err());
        assert!(
            decode_journal(&vec![0; 5000], 9, 1)
                .unwrap_err()
                .to_string()
                .contains("encoded budget")
        );
        assert!(decode_commit(br#"{"version":99}"#, Some(&journal), 9, 100).is_err());
    }
    #[test]
    fn deployed_inline_v2_commit_requires_no_new_journal_table_value() {
        let pending = PendingCommit {
            version: PROTOCOL_VERSION,
            generation: "deployed".into(),
            epoch: 9,
            transaction_index: 4,
            records: vec![record()],
        };
        let inline = serde_json::to_vec(&pending).unwrap();
        assert_eq!(
            decode_commit(&inline, None, 9, 100).unwrap().records,
            pending.records
        );
    }
}
