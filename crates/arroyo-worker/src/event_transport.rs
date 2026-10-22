//! Response-only attempt binding. Ordinary Data/Header framing is untouched.
use crate::event_response_codec::{EXTENSION_BYTES, EncodedResponse, ResponseSpec};
pub use crate::network_manager::{NetworkManager, Quad, Senders};
use anyhow::{Context, Result, ensure};
use arroyo_rpc::grpc::rpc::TaskAssignment;
use arroyo_types::event_edge::EventEdgeSpec;
use arroyo_types::event_response::EventResponse;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const ATTEMPT_BYTES: usize = 24;
const WIRE_EXTENSION_BYTES: usize = ATTEMPT_BYTES + EXTENSION_BYTES;
// Existing bincode standard Signal encoding only: tag1 + epoch5 + min_epoch5
// + SystemTime(Duration secs9/nanos5) + bool1. Watermark (including PreEpoch)
// is at most outertag1 + watermarktag1 + Duration14. No new Signal variants.
const MAX_SIGNAL_BYTES: usize = 1 + 5 + 5 + 9 + 5 + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EventAttempt {
    generation: u64,
    source_worker: u64,
    destination_worker: u64,
}
impl EventAttempt {
    fn from_assignments(generation: u64, quad: Quad, tasks: &[TaskAssignment]) -> Result<Self> {
        let worker = |id: usize, subtask: usize| -> Result<u64> {
            let mut matching = tasks
                .iter()
                .filter(|task| task.task_id as usize == id && task.subtask_idx as usize == subtask);
            let result = matching
                .next()
                .context("missing response task assignment")?
                .worker_id;
            ensure!(
                matching.next().is_none(),
                "duplicate response task assignment"
            );
            Ok(result)
        };
        for field in [quad.src_id, quad.src_idx, quad.dst_id, quad.dst_idx] {
            u32::try_from(field).context("response Quad exceeds existing u32 header")?;
        }
        Ok(Self {
            generation,
            source_worker: worker(quad.src_id, quad.src_idx)?,
            destination_worker: worker(quad.dst_id, quad.dst_idx)?,
        })
    }
    fn encode(self) -> [u8; ATTEMPT_BYTES] {
        let mut result = [0; ATTEMPT_BYTES];
        result[..8].copy_from_slice(&self.generation.to_le_bytes());
        result[8..16].copy_from_slice(&self.source_worker.to_le_bytes());
        result[16..24].copy_from_slice(&self.destination_worker.to_le_bytes());
        result
    }
    fn validate(self, input: &[u8]) -> Result<()> {
        ensure!(
            input == self.encode(),
            "response execution assignment mismatch"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct EventWireSpec {
    quad: Quad,
    attempt: EventAttempt,
    edge: Arc<EventEdgeSpec>,
    codec: ResponseSpec,
}
impl EventWireSpec {
    /// Engine supplies CURRENT WorkerContext generation and CURRENT task
    /// assignments. Never infer execution identity from packet/root nonce.
    pub fn registered(
        generation: u64,
        quad: Quad,
        tasks: &[TaskAssignment],
        edge: Arc<EventEdgeSpec>,
    ) -> Result<Self> {
        Ok(Self {
            quad,
            attempt: EventAttempt::from_assignments(generation, quad, tasks)?,
            codec: ResponseSpec::from_edge(&edge)?,
            edge,
        })
    }
    /// Intended Engine/embedding input registration. No operator is activated.
    pub fn register_input(
        &self,
        senders: &mut Senders,
        tx: arroyo_operator::context::BatchSender,
    ) -> Result<()> {
        senders.add_response(self.quad, Arc::new(self.clone()), tx, None)
    }
    /// Standalone Engine/embedding output registration; worker scheduling uses
    /// the same internal reported path with its existing failure reporter.
    pub async fn connect_output(
        &self,
        network: &NetworkManager,
        address: &str,
        rx: arroyo_operator::context::BatchReceiver,
    ) {
        network
            .connect_response_reported(address, self.quad, rx, None, Arc::new(self.clone()))
            .await;
    }
    /// Checked physical column forwarding for the registered next stage.
    pub fn rebind_rows(
        &self,
        response: &EventResponse,
        rows: Option<arrow_array::RecordBatch>,
    ) -> Result<EventResponse> {
        crate::event_response_codec::rebind_response_rows(response, rows, &self.edge)
    }
    pub(crate) fn require_quad(&self, quad: Quad) -> Result<()> {
        ensure!(self.quad == quad, "response descriptor Quad mismatch");
        Ok(())
    }
    pub(crate) fn schema(&self) -> arrow_schema::SchemaRef {
        self.edge.schema.clone()
    }
    pub(crate) async fn write_identity<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.attempt.encode()).await?;
        Ok(())
    }
    pub(crate) fn encode_signal(&self, signal: &arroyo_types::SignalMessage) -> Result<Vec<u8>> {
        let bytes = bincode::encode_to_vec(signal, bincode::config::standard())?;
        ensure!(
            !bytes.is_empty() && bytes.len() <= MAX_SIGNAL_BYTES,
            "response Signal exceeds canonical type bound"
        );
        Ok(bytes)
    }
    pub(crate) async fn read_signal_payload<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        bytes: usize,
    ) -> Result<arroyo_types::SignalMessage> {
        let mut identity = [0; ATTEMPT_BYTES];
        reader.read_exact(&mut identity).await?;
        self.attempt.validate(&identity)?;
        ensure!(
            bytes > 0 && bytes <= MAX_SIGNAL_BYTES,
            "response Signal exceeds canonical type bound"
        );
        let mut payload = [0; MAX_SIGNAL_BYTES];
        reader.read_exact(&mut payload[..bytes]).await?;
        let (signal, consumed) =
            bincode::decode_from_slice(&payload[..bytes], bincode::config::standard())?;
        ensure!(consumed == bytes, "trailing response Signal bytes");
        Ok(signal)
    }
    pub(crate) async fn write_payload<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        encoded: &EncodedResponse,
    ) -> Result<()> {
        writer.write_all(&self.attempt.encode()).await?;
        writer.write_all(&encoded.extension).await?;
        writer.write_all(&encoded.keys).await?;
        writer.write_all(&encoded.rows).await?;
        Ok(())
    }
    pub(crate) fn encode(&self, response: &EventResponse) -> Result<EncodedResponse> {
        self.edge.validate(response).map_err(anyhow::Error::msg)?;
        self.codec.encode(response)
    }
    pub(crate) async fn read_payload<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        row_bytes: u32,
    ) -> Result<EventResponse> {
        let mut fixed = [0; WIRE_EXTENSION_BYTES];
        reader.read_exact(&mut fixed).await?;
        // Exact old generation/source/destination assignment rejection precedes
        // ALL variable frame allocation and IPC decoding. Root nonce latching
        // is not being used as a freshness/security substitute.
        self.attempt.validate(&fixed[..ATTEMPT_BYTES])?;
        let extension: [u8; EXTENSION_BYTES] = fixed[ATTEMPT_BYTES..].try_into()?;
        let (_, key_bytes) = self.codec.preflight(&extension, row_bytes)?;
        let mut encoded = EncodedResponse {
            extension,
            keys: vec![0; key_bytes],
            rows: vec![0; row_bytes as usize],
        };
        reader.read_exact(&mut encoded.keys).await?;
        reader.read_exact(&mut encoded.rows).await?;
        let response = self.codec.decode(&encoded)?;
        self.edge.validate(&response).map_err(anyhow::Error::msg)?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arroyo_types::event_edge::EventLayout;
    use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext};
    use std::time::Duration;
    fn quad() -> Quad {
        Quad {
            src_id: 1,
            src_idx: 0,
            dst_id: 2,
            dst_idx: 0,
        }
    }
    fn tasks() -> Vec<TaskAssignment> {
        [(1, 100), (2, 200)]
            .into_iter()
            .map(|(task_id, worker_id)| TaskAssignment {
                task_id,
                subtask_idx: 0,
                worker_id,
                worker_addr: String::new(),
                worker_rpc: String::new(),
            })
            .collect()
    }
    fn edge() -> Arc<EventEdgeSpec> {
        Arc::new(EventEdgeSpec {
            root_operator: 7,
            root_subtask: 0,
            schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            layout: EventLayout::Raw,
        })
    }
    fn empty() -> EventResponse {
        EventResponse {
            native_body: None,
            context: EventContext {
                version: EVENT_RESPONSE_VERSION,
                root_operator: 7,
                root_subtask: 0,
                attempt_nonce: [9; 16],
                sequence: 1,
                trigger_nanos: 5,
            },
            rows: None,
            touched_keys: None,
        }
    }
    #[tokio::test]
    async fn event_transport_empty_roundtrip_keeps_pure48_and_wire72() -> Result<()> {
        let registered = EventWireSpec::registered(3, quad(), &tasks(), edge())?;
        let encoded = registered.encode(&empty())?;
        let mut wire = Vec::new();
        registered.write_payload(&mut wire, &encoded).await?;
        assert_eq!(EXTENSION_BYTES, 48);
        assert_eq!(WIRE_EXTENSION_BYTES, 72);
        assert_eq!(wire.len(), 72);
        assert_eq!(
            registered.read_payload(&mut wire.as_slice(), 0).await?,
            empty()
        );
        Ok(())
    }
    #[tokio::test]
    async fn event_transport_stale_generation_or_worker_rejects_without_variable_body() -> Result<()>
    {
        let fresh = EventWireSpec::registered(4, quad(), &tasks(), edge())?;
        let mut old_workers = tasks();
        old_workers[0].worker_id = 99;
        let mut old_destination = tasks();
        old_destination[1].worker_id = 199;
        for stale in [
            EventWireSpec::registered(3, quad(), &tasks(), edge())?,
            EventWireSpec::registered(4, quad(), &old_workers, edge())?,
            EventWireSpec::registered(4, quad(), &old_destination, edge())?,
        ] {
            let encoded = stale.encode(&empty())?;
            let (mut tx, mut rx) = tokio::io::duplex(128);
            stale.write_payload(&mut tx, &encoded).await?;
            // Declare a finite variable body, send none, and keep sender open.
            // A read-before-identity bug cannot qualify as a semantic rejection.
            // The full-u32 boundary remains the pure codec's nonallocating test.
            let error =
                tokio::time::timeout(Duration::from_secs(1), fresh.read_payload(&mut rx, 65_536))
                    .await?
                    .err()
                    .context("stale assignment must fail")?;
            assert_eq!(error.to_string(), "response execution assignment mismatch");
        }
        Ok(())
    }
    #[test]
    fn event_transport_requires_exact_existing_task_assignment() -> Result<()> {
        let mut missing = tasks();
        missing.pop();
        assert!(EventWireSpec::registered(0, quad(), &missing, edge()).is_err());
        let mut duplicated = tasks();
        duplicated.push(duplicated[0].clone());
        assert!(EventWireSpec::registered(0, quad(), &duplicated, edge()).is_err());
        let zero = EventWireSpec::registered(0, quad(), &tasks(), edge())?;
        zero.require_quad(quad())?;
        let wrong = Quad {
            dst_id: 3,
            ..quad()
        };
        assert_eq!(
            zero.require_quad(wrong).unwrap_err().to_string(),
            "response descriptor Quad mismatch"
        );
        EventWireSpec::registered(u64::MAX, quad(), &tasks(), edge())?;
        Ok(())
    }
}

#[cfg(test)]
mod repaired_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
    use arroyo_types::event_edge::EventLayout;
    use arroyo_types::event_response::{EVENT_RESPONSE_VERSION, EventContext};
    use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
    use std::time::{Duration, UNIX_EPOCH};
    fn quad() -> Quad {
        Quad {
            src_id: 1,
            src_idx: 0,
            dst_id: 2,
            dst_idx: 0,
        }
    }
    fn tasks() -> Vec<TaskAssignment> {
        [(1, 100), (2, 200)]
            .into_iter()
            .map(|(task_id, worker_id)| TaskAssignment {
                task_id,
                worker_id,
                subtask_idx: 0,
                worker_addr: String::new(),
                worker_rpc: String::new(),
            })
            .collect()
    }
    fn empty() -> EventResponse {
        EventResponse {
            native_body: None,
            context: EventContext {
                version: EVENT_RESPONSE_VERSION,
                root_operator: 7,
                root_subtask: 0,
                attempt_nonce: [9; 16],
                sequence: 1,
                trigger_nanos: 5,
            },
            rows: None,
            touched_keys: None,
        }
    }
    fn raw() -> EventWireSpec {
        EventWireSpec::registered(
            4,
            quad(),
            &tasks(),
            Arc::new(EventEdgeSpec {
                root_operator: 7,
                root_subtask: 0,
                schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
                layout: EventLayout::Raw,
            }),
        )
        .unwrap()
    }
    #[tokio::test]
    async fn event_transport_exact_owner_utf8_and_multicolumn_receive_and_forward() -> Result<()> {
        for multiple in [false, true] {
            let text = "é界".repeat(8192);
            let mut fields = vec![Field::new("text", DataType::Utf8, true)];
            let mut columns =
                vec![Arc::new(StringArray::from(vec![Some(text.as_str()), None])) as ArrayRef];
            if multiple {
                fields.extend([
                    Field::new("bytes", DataType::Binary, true),
                    Field::new("count", DataType::Int64, false),
                ]);
                columns.extend([
                    Arc::new(BinaryArray::from(vec![Some(text.as_bytes()), None])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                ]);
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
            let allowance = batch.get_array_memory_size();
            let edge = Arc::new(EventEdgeSpec {
                root_operator: 7,
                root_subtask: 0,
                schema: batch.schema(),
                layout: EventLayout::Native {
                    max_rows: 4,
                    owner_output_bytes: allowance,
                    encoded_key_bytes: 512,
                },
            });
            let registered = EventWireSpec::registered(4, quad(), &tasks(), edge.clone())?;
            let response = EventResponse {
                rows: Some(batch.clone()),
                ..empty()
            };
            let encoded = registered.encode(&response)?;
            let mut wire = Vec::new();
            registered.write_payload(&mut wire, &encoded).await?;
            let decoded = registered
                .read_payload(&mut wire.as_slice(), u32::try_from(encoded.rows.len())?)
                .await?;
            assert_eq!(decoded, response);
            assert!(
                decoded.rows.as_ref().unwrap().get_array_memory_size() > allowance,
                "fixture must expose shared IPC capacity double counting"
            );
            assert!(decoded.is_native_forward(&edge));
            let forwarded = registered.encode(&decoded)?;
            let mut next_wire = Vec::new();
            registered.write_payload(&mut next_wire, &forwarded).await?;
            assert_eq!(
                registered
                    .read_payload(
                        &mut next_wire.as_slice(),
                        u32::try_from(forwarded.rows.len())?
                    )
                    .await?,
                response
            );
            let smaller = EventEdgeSpec {
                layout: EventLayout::Native {
                    max_rows: 4,
                    owner_output_bytes: allowance - 1,
                    encoded_key_bytes: 512,
                },
                ..edge.as_ref().clone()
            };
            assert_eq!(
                smaller.validate(&response).unwrap_err(),
                "native sender exceeds its existing output allowance"
            );
            let mut replaced = decoded.clone();
            replaced.rows = Some(batch);
            assert!(!replaced.is_native_forward(&edge));
            // A reused proof cannot authorize a new independently allocated
            // oversized output, even if its schema and event identity match.
            let too_large = "x".repeat(allowance * 2);
            let mut replaced_columns = replaced.rows.as_ref().unwrap().columns().to_vec();
            replaced_columns[0] = Arc::new(StringArray::from(vec![Some(too_large.as_str()), None]));
            replaced.rows = Some(RecordBatch::try_new(edge.schema.clone(), replaced_columns)?);
            assert_eq!(
                edge.validate(&replaced).unwrap_err(),
                "native sender exceeds its existing output allowance"
            );
            let mut changed_context = decoded.clone();
            changed_context.context.sequence += 1;
            assert!(!changed_context.is_native_forward(&edge));
            assert_eq!(
                edge.validate(&changed_context).unwrap_err(),
                "native sender exceeds its existing output allowance"
            );
            let fake_body: arrow::buffer::Buffer =
                arrow::buffer::MutableBuffer::from_len_zeroed(1).into();
            let error = decoded
                .clone()
                .with_native_body(&edge, fake_body)
                .unwrap_err();
            assert_eq!(error, "native row buffer is not owned by proved body");
        }
        Ok(())
    }
    #[tokio::test]
    async fn event_transport_checked_projection_alias_subset_slice_and_compact() -> Result<()> {
        let text = "é界".repeat(8192);
        let owned = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("count", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some(text.as_str()), None])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![Some(text.as_bytes()), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )?;
        let allowance = owned.get_array_memory_size();
        let edge = Arc::new(EventEdgeSpec {
            root_operator: 7,
            root_subtask: 0,
            schema: owned.schema(),
            layout: EventLayout::Native {
                max_rows: 4,
                owner_output_bytes: allowance,
                encoded_key_bytes: 512,
            },
        });
        let registered = EventWireSpec::registered(4, quad(), &tasks(), edge.clone())?;
        let original = EventResponse {
            rows: Some(owned.clone()),
            ..empty()
        };
        let encoded = registered.encode(&original)?;
        let mut wire = Vec::new();
        registered.write_payload(&mut wire, &encoded).await?;
        let decoded = registered
            .read_payload(&mut wire.as_slice(), u32::try_from(encoded.rows.len())?)
            .await?;
        let batch = decoded.rows.as_ref().unwrap();
        // Rebuilt batches/renamed fields are not Arc<RecordBatch> identity.
        // These projections still borrow exact immutable source columns.
        for (indices, slice) in [
            (vec![0, 1, 2], false),
            (vec![2, 0, 1], false),
            (vec![0], false),
            (vec![0], true),
        ] {
            let fields = indices
                .iter()
                .enumerate()
                .map(|(i, index)| {
                    let field = owned.schema().field(*index).clone();
                    field.with_name(format!("alias_{i}"))
                })
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let projected = RecordBatch::try_new(
                schema.clone(),
                indices
                    .iter()
                    .map(|index| {
                        if slice {
                            batch.column(*index).slice(1, 1)
                        } else {
                            batch.column(*index).clone()
                        }
                    })
                    .collect(),
            )?;
            let next = Arc::new(EventEdgeSpec {
                schema,
                ..edge.as_ref().clone()
            });
            assert!(
                projected.get_array_memory_size() > allowance,
                "must need backing proof"
            );
            assert!(decoded.proves_native_projection(&next, &projected));
            let next_wire = EventWireSpec::registered(4, quad(), &tasks(), next.clone())?;
            let rebound = next_wire.rebind_rows(&decoded, Some(projected.clone()))?;
            assert_eq!(rebound.context, original.context);
            assert_eq!(rebound.touched_keys, original.touched_keys);
            assert_eq!(rebound.rows.as_ref(), Some(&projected));
            assert!(rebound.is_native_forward(&next));
            let onward = next_wire.encode(&rebound)?;
            let mut wire = Vec::new();
            next_wire.write_payload(&mut wire, &onward).await?;
            let received = next_wire
                .read_payload(&mut wire.as_slice(), u32::try_from(onward.rows.len())?)
                .await?;
            assert_eq!(received, rebound);
            assert!(received.is_native_forward(&next));
        }
        // Selecting only a tiny integer column must not retain the old large
        // allocation under a smaller valid output stage. Compact through the
        // canonical bounded writer; this changes buffers, not values.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "small",
            DataType::Int64,
            false,
        )]));
        let fresh = RecordBatch::try_new(schema.clone(), vec![owned.column(2).clone()])?;
        let next = Arc::new(EventEdgeSpec {
            schema: schema.clone(),
            layout: EventLayout::Native {
                max_rows: 4,
                owner_output_bytes: fresh.get_array_memory_size(),
                encoded_key_bytes: 512,
            },
            ..edge.as_ref().clone()
        });
        let projected = RecordBatch::try_new(schema.clone(), vec![batch.column(2).clone()])?;
        assert!(decoded.proves_native_projection(&next, &projected));
        assert_eq!(
            decoded
                .try_rebind_native_rows(&next, Some(projected.clone()))
                .unwrap_err(),
            "native body allocation exceeds registered proof"
        );
        let small_wire = EventWireSpec::registered(4, quad(), &tasks(), next.clone())?;
        let compact = small_wire.rebind_rows(&decoded, Some(projected.clone()))?;
        assert_eq!(compact.rows.as_ref(), Some(&fresh));
        assert!(compact.is_native_forward(&next));
        assert!(!Arc::ptr_eq(
            compact.rows.as_ref().unwrap().column(0),
            projected.column(0)
        ));
        let encoded = small_wire.encode(&compact)?;
        let metadata = u32::from_le_bytes(encoded.rows[..4].try_into()?) as usize;
        assert_eq!(
            encoded.rows.len() - 4 - metadata,
            arroyo_types::event_edge::native_canonical_body_len(&fresh)
                .map_err(anyhow::Error::msg)?
        );
        // Independent equal arrays are not column-forwarding provenance. An
        // oversized computed result must still pass original owner admission.
        let large = "x".repeat(allowance * 2);
        let computed_schema = Arc::new(Schema::new(vec![Field::new(
            "computed",
            DataType::Utf8,
            false,
        )]));
        let computed = RecordBatch::try_new(
            computed_schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                large.as_str(),
                large.as_str(),
            ]))],
        )?;
        let computed_edge = Arc::new(EventEdgeSpec {
            schema: computed_schema,
            ..edge.as_ref().clone()
        });
        assert!(!decoded.proves_native_projection(&computed_edge, &computed));
        let computed_wire = EventWireSpec::registered(4, quad(), &tasks(), computed_edge)?;
        assert_eq!(
            computed_wire
                .rebind_rows(&decoded, Some(computed))
                .unwrap_err()
                .to_string(),
            "native row change requires original owner admission"
        );
        let mut stale = decoded.clone();
        stale.context.sequence += 1;
        assert!(!stale.proves_native_projection(&next, &projected));
        Ok(())
    }
    #[tokio::test]
    async fn event_transport_duplicate_large_alias_rejects_before_canonical_writer() -> Result<()> {
        let text = "é界".repeat(8192);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some(text.as_str()), None]))],
        )?;
        let allowance = batch.get_array_memory_size();
        let edge = Arc::new(EventEdgeSpec {
            root_operator: 7,
            root_subtask: 0,
            schema: batch.schema(),
            layout: EventLayout::Native {
                max_rows: 4,
                owner_output_bytes: allowance,
                encoded_key_bytes: 512,
            },
        });
        let registered = EventWireSpec::registered(4, quad(), &tasks(), edge.clone())?;
        let encoded = registered.encode(&EventResponse {
            rows: Some(batch),
            ..empty()
        })?;
        let mut wire = Vec::new();
        registered.write_payload(&mut wire, &encoded).await?;
        let decoded = registered
            .read_payload(&mut wire.as_slice(), u32::try_from(encoded.rows.len())?)
            .await?;
        let rows = decoded.rows.as_ref().unwrap();
        let alias_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let repeated = RecordBatch::try_new(
            alias_schema.clone(),
            vec![rows.column(0).clone(), rows.column(0).clone()],
        )?;
        let next = Arc::new(EventEdgeSpec {
            schema: alias_schema,
            ..edge.as_ref().clone()
        });
        let next_wire = EventWireSpec::registered(4, quad(), &tasks(), next.clone())?;
        // The old allocation fits once, but canonical IPC copies both aliases.
        let rebound = next_wire.rebind_rows(&decoded, Some(repeated))?;
        assert!(rebound.is_native_forward(&next));
        let (nodes, buffers) = arroyo_types::event_edge::native_schema_shape(&next.schema)
            .map_err(anyhow::Error::msg)?;
        let bound = arroyo_types::event_edge::native_body_bound(allowance, 4, nodes, buffers)
            .map_err(anyhow::Error::msg)?;
        assert!(
            arroyo_types::event_edge::native_canonical_body_len(rebound.rows.as_ref().unwrap())
                .map_err(anyhow::Error::msg)?
                > bound
        );
        assert_eq!(
            next_wire
                .encode(&rebound)
                .err()
                .context("duplicate aliases must exceed canonical body proof")?
                .to_string(),
            "native canonical body exceeds registered proof before encoding"
        );
        Ok(())
    }
    fn signals() -> Vec<SignalMessage> {
        // 2^32 seconds hits the nine-byte u64 varint width while remaining
        // comfortably representable by this Linux SystemTime implementation.
        let span = Duration::new(u64::from(u32::MAX) + 1, 999_999_999);
        vec![
            SignalMessage::Barrier(CheckpointBarrier {
                epoch: u32::MAX,
                min_epoch: u32::MAX,
                timestamp: UNIX_EPOCH.checked_add(span).unwrap(),
                then_stop: true,
            }),
            SignalMessage::Watermark(Watermark::EventTime(UNIX_EPOCH.checked_add(span).unwrap())),
            SignalMessage::Watermark(Watermark::EventTime(UNIX_EPOCH.checked_sub(span).unwrap())),
            SignalMessage::Watermark(Watermark::Idle),
            SignalMessage::Stop,
            SignalMessage::EndOfData,
        ]
    }
    #[tokio::test]
    async fn event_transport_bound_signals_cover_maximal_existing_variants_and_consumption()
    -> Result<()> {
        let registered = raw();
        let mut maximum = 0;
        for signal in signals() {
            let encoded = registered.encode_signal(&signal)?;
            maximum = maximum.max(encoded.len());
            let mut wire = Vec::new();
            registered.write_identity(&mut wire).await?;
            wire.extend_from_slice(&encoded);
            assert_eq!(
                registered
                    .read_signal_payload(&mut wire.as_slice(), encoded.len())
                    .await?,
                signal
            );
        }
        assert_eq!(maximum, 26);
        assert_eq!(MAX_SIGNAL_BYTES, 26);
        let mut wire = Vec::new();
        registered.write_identity(&mut wire).await?;
        wire.extend_from_slice(&registered.encode_signal(&SignalMessage::Stop)?);
        wire.push(0);
        let error = registered
            .read_signal_payload(&mut wire.as_slice(), 2)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "trailing response Signal bytes");
        for length in [0, MAX_SIGNAL_BYTES + 1] {
            let mut wire = Vec::new();
            registered.write_identity(&mut wire).await?;
            assert_eq!(
                registered
                    .read_signal_payload(&mut wire.as_slice(), length)
                    .await
                    .unwrap_err()
                    .to_string(),
                "response Signal exceeds canonical type bound"
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn event_transport_stale_signal_identity_rejects_without_payload() -> Result<()> {
        let registered = raw();
        let old = EventWireSpec::registered(3, quad(), &tasks(), registered.edge.clone())?;
        let (mut tx, mut rx) = tokio::io::duplex(64);
        old.write_identity(&mut tx).await?;
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            registered.read_signal_payload(&mut rx, MAX_SIGNAL_BYTES),
        )
        .await?
        .unwrap_err();
        assert_eq!(error.to_string(), "response execution assignment mismatch");
        Ok(())
    }
}
