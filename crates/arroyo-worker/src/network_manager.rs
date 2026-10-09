#![allow(clippy::redundant_slicing)]
use crate::arrow::execution::{ExecutionResources, configured_execution_resources};
use anyhow::{Context, anyhow, bail};
use arrow::buffer::MutableBuffer;
use arrow::ipc::reader::read_record_batch;
use arrow::ipc::writer::{DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use arroyo_rpc::ControlResp;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_types::ArrowMessage;
use bincode::config;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::MemoryReservation;
use std::net::SocketAddr;
use std::{collections::HashMap, mem::size_of, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{self, AsyncRead, AsyncWrite, BufReader, BufWriter},
    select,
    sync::Mutex,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

use bytes::{Buf, BufMut};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use arroyo_operator::context::{BatchReceiver, BatchSender};
use tokio::time::{Interval, interval};
use tokio_rustls::rustls::pki_types::{IpAddr, ServerName};
use tokio_stream::StreamExt;

use arroyo_operator::inq_reader::InQReader;
use arroyo_rpc::config::{TlsConfig, config};
use arroyo_rpc::intern;
use arroyo_server_common::shutdown::ShutdownGuard;
use url::{Host, Url};

#[derive(Clone, Default)]
pub(crate) struct NetworkTasks(Arc<std::sync::Mutex<NetworkTaskState>>);

#[derive(Default)]
struct NetworkTaskState {
    cancelled: bool,
    handles: Vec<tokio::task::AbortHandle>,
}

impl NetworkTasks {
    fn register(&self, handle: tokio::task::AbortHandle) {
        let mut tasks = self.0.lock().unwrap();
        if tasks.cancelled {
            handle.abort();
            return;
        }
        tasks.handles.retain(|task| !task.is_finished());
        tasks.handles.push(handle);
    }

    pub(crate) fn abort(&self) {
        let mut tasks = self.0.lock().unwrap();
        tasks.cancelled = true;
        for task in tasks.handles.drain(..) {
            task.abort();
        }
    }
}

// All network permits use the configured operator/graph pool, never a separate
// transfer allowance. They fail immediately: waiting for memory while holding
// upstream/output permits can deadlock a full pipeline.
fn reserve_network(
    resources: Option<&ExecutionResources>,
    name: &str,
    bytes: usize,
) -> DataflowResult<Option<MemoryReservation>> {
    resources
        .map(|resources| {
            let bytes = bytes.checked_add(256).ok_or_else(network_size_overflow)?;
            resources
                .reserve_bytes(name, bytes)
                .map_err(DataflowError::from)
        })
        .transpose()
}

fn network_size_overflow() -> DataflowError {
    DataFusionError::ResourcesExhausted("network buffer admission size overflow".into()).into()
}

fn network_io_error(error: io::Error) -> DataflowError {
    DataflowError::ExternalError(format!("network I/O: {error}"))
}

#[derive(Clone)]
pub(crate) struct NetworkFailureReporter {
    control: tokio::sync::mpsc::Sender<ControlResp>,
    task_id: u32,
    subtask_idx: u32,
}

impl NetworkFailureReporter {
    pub(crate) fn new(
        control: tokio::sync::mpsc::Sender<ControlResp>,
        task_id: u32,
        subtask_idx: usize,
    ) -> Self {
        Self {
            control,
            task_id,
            subtask_idx: subtask_idx as u32,
        }
    }

    async fn report(&self, error: DataflowError) {
        self.control
            .send(ControlResp::TaskFailed {
                task_id: self.task_id,
                subtask_idx: self.subtask_idx,
                error: error.into(),
            })
            .await
            .ok();
    }
}

fn schema_depth(data_type: &arrow_schema::DataType) -> usize {
    use arrow_schema::DataType;
    let child = match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => schema_depth(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| schema_depth(field.data_type()))
            .max()
            .unwrap_or(0),
        DataType::Union(fields, _) => fields
            .iter()
            .map(|(_, field)| schema_depth(field.data_type()))
            .max()
            .unwrap_or(0),
        DataType::RunEndEncoded(left, right) => {
            schema_depth(left.data_type()).max(schema_depth(right.data_type()))
        }
        DataType::Dictionary(_, value) => schema_depth(value),
        _ => 0,
    };
    child.saturating_add(1)
}

fn reserve_encode(
    resources: Option<&ExecutionResources>,
    batch: &RecordBatch,
) -> DataflowResult<Option<MemoryReservation>> {
    let Some(resources) = resources else {
        return Ok(None);
    };
    // Arrow55 writer.rs write_array_data creates validity bytes for all-valid
    // arrays, normalized offset/bit buffers, IPC descriptors, a growing Vec and
    // a growing flatbuffer. Include every child and ancestor clone, including
    // zero-byte Null children. Eight copies cover growth + old/new allocations.
    fn shape(data: &arrow::array::ArrayData, depth: usize) -> Option<usize> {
        use arrow_schema::DataType;
        let validity = if matches!(
            data.data_type(),
            DataType::Null | DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
        ) {
            0
        } else {
            data.len().div_ceil(8)
        };
        let mut bytes = data
            .buffers()
            .len()
            .checked_mul(128)?
            .checked_add(1024)?
            .checked_mul(depth)?
            .checked_add(validity)?;
        for child in data.child_data() {
            bytes = bytes.checked_add(shape(child, depth.checked_add(1)?)?)?;
        }
        Some(bytes)
    }
    let mut bytes = batch.get_array_memory_size();
    for column in batch.columns() {
        bytes = bytes
            .checked_add(shape(&column.to_data(), 1).ok_or_else(network_size_overflow)?)
            .ok_or_else(network_size_overflow)?;
    }
    reserve_network(
        Some(resources),
        "network IPC encoding",
        bytes.checked_mul(8).ok_or_else(network_size_overflow)?,
    )
}

fn ipc_parts(data: &[u8]) -> anyhow::Result<(&[u8], &[u8])> {
    let prefix: [u8; 4] = data.get(..4).context("truncated IPC length")?.try_into()?;
    let length = u32::from_le_bytes(prefix) as usize;
    let end = 4usize.checked_add(length).context("IPC length overflow")?;
    Ok((
        data.get(4..end).context("truncated IPC metadata")?,
        data.get(end..).context("truncated IPC body")?,
    ))
}

fn reserve_decode(
    resources: Option<&ExecutionResources>,
    schema: &SchemaRef,
    data: &[u8],
) -> DataflowResult<Option<MemoryReservation>> {
    let Some(resources) = resources else {
        return Ok(None);
    };
    let (metadata, body) = ipc_parts(data)?;
    let message = arrow::ipc::root_as_message(metadata)
        .map_err(|error| anyhow!("invalid IPC metadata: {error}"))?;
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| anyhow!("expected IPC record batch"))?;
    // Our writer uses uncompressed IPC. Without an audited decompression bound,
    // an incoming compressed frame must fail before Arrow can allocate its body.
    if batch.compression().is_some() {
        return Err(DataFusionError::ResourcesExhausted(
            "compressed network IPC has no configured allocation bound".into(),
        )
        .into());
    }
    let body_len = usize::try_from(message.bodyLength()).map_err(|_| network_size_overflow())?;
    if body_len != body.len() {
        return Err(anyhow!("IPC body length mismatch").into());
    }
    let depth = schema
        .fields()
        .iter()
        .map(|field| schema_depth(field.data_type()))
        .max()
        .unwrap_or(1)
        .checked_add(1)
        .ok_or_else(network_size_overflow)?;
    let nodes = batch.nodes().map(|nodes| nodes.len()).unwrap_or(0);
    let buffers = batch
        .buffers()
        .ok_or_else(|| anyhow!("missing IPC buffers"))?;
    let mut copied = 0usize;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset()).map_err(|_| network_size_overflow())?;
        let len = usize::try_from(buffer.length()).map_err(|_| network_size_overflow())?;
        if offset.checked_add(len).is_none_or(|end| end > body_len) {
            return Err(anyhow!("IPC buffer outside body").into());
        }
        // Alignment repair may copy a buffer. Repeated/overlapping descriptors
        // are counted independently rather than assuming bodyLength bounds them.
        copied = copied
            .checked_add(len.checked_add(63).ok_or_else(network_size_overflow)?)
            .ok_or_else(network_size_overflow)?;
    }
    let metadata = nodes
        .checked_mul(1024)
        .and_then(|n| {
            buffers
                .len()
                .checked_mul(128)
                .and_then(|b| n.checked_add(b))
        })
        .and_then(|bytes| bytes.checked_mul(depth))
        .ok_or_else(network_size_overflow)?;
    let bytes = body_len
        .checked_add(63)
        .and_then(|bytes| {
            copied
                .checked_mul(2)
                .and_then(|copied| bytes.checked_add(copied))
        })
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or_else(network_size_overflow)?;
    reserve_network(Some(resources), "network IPC decoding", bytes)
}

// Abstraction for stream types that can be either TLS or plain TCP
#[derive(Debug)]
enum NetworkStream {
    Plain(TcpStream),
    TlsClient(tokio_rustls::client::TlsStream<TcpStream>),
    TlsServer(tokio_rustls::server::TlsStream<TcpStream>),
}

impl NetworkStream {
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            NetworkStream::Plain(p) => p.local_addr(),
            NetworkStream::TlsClient(c) => c.get_ref().0.local_addr(),
            NetworkStream::TlsServer(s) => s.get_ref().0.local_addr(),
        }
    }
}

impl AsyncRead for NetworkStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            NetworkStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            NetworkStream::TlsClient(stream) => Pin::new(stream).poll_read(cx, buf),
            NetworkStream::TlsServer(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for NetworkStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            NetworkStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            NetworkStream::TlsClient(stream) => Pin::new(stream).poll_write(cx, buf),
            NetworkStream::TlsServer(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        match self.get_mut() {
            NetworkStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            NetworkStream::TlsClient(stream) => Pin::new(stream).poll_flush(cx),
            NetworkStream::TlsServer(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        match self.get_mut() {
            NetworkStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            NetworkStream::TlsClient(stream) => Pin::new(stream).poll_shutdown(cx),
            NetworkStream::TlsServer(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[derive(Clone)]
struct NetworkSender {
    tx: BatchSender,
    schema: SchemaRef,
    failure: Option<NetworkFailureReporter>,
}

#[derive(Clone)]
pub struct Senders {
    senders: Arc<HashMap<Quad, NetworkSender>>,
}

impl Default for Senders {
    fn default() -> Self {
        Self::new()
    }
}

impl Senders {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(HashMap::new()),
        }
    }

    pub fn merge(&mut self, other: Self) {
        Arc::make_mut(&mut self.senders).extend(Arc::unwrap_or_clone(other.senders))
    }

    pub fn add(&mut self, quad: Quad, schema: SchemaRef, tx: BatchSender) {
        Arc::make_mut(&mut self.senders).insert(
            quad,
            NetworkSender {
                tx,
                schema,
                failure: None,
            },
        );
    }

    pub(crate) fn add_reported(
        &mut self,
        quad: Quad,
        schema: SchemaRef,
        tx: BatchSender,
        failure: NetworkFailureReporter,
    ) {
        Arc::make_mut(&mut self.senders).insert(
            quad,
            NetworkSender {
                tx,
                schema,
                failure: Some(failure),
            },
        );
    }

    async fn send(
        &mut self,
        header: Header,
        data: Vec<u8>,
        resources: Option<&ExecutionResources>,
    ) -> DataflowResult<()> {
        let sender = self
            .senders
            .get(&header.as_quad())
            .ok_or_else(|| DataflowError::ArgumentError("unknown network destination".into()))?;
        let mut decoded = None;
        let message = match header.message_type {
            MessageType::Data => {
                decoded = reserve_decode(resources, &sender.schema, &data)?;
                let batch = read_message(sender.schema.clone(), data)?;
                if let Some(resources) = resources {
                    resources.check_batch("network decoded input", &batch)?;
                }
                ArrowMessage::Data(batch)
            }
            MessageType::Signal => ArrowMessage::Signal(
                bincode::decode_from_slice(&data, config::standard())
                    .map_err(|error| DataflowError::DataError {
                        details: error.to_string(),
                        count: 1,
                    })?
                    .0,
            ),
        };
        let end = message.is_end();
        let result = sender.tx.send_checked(message).await;
        drop(decoded);
        match result {
            Err(
                error @ DataflowError::InternalOperatorError {
                    error: "downstream graph queue closed",
                    ..
                },
            ) if end => {
                warn!("couldn't send end message: {error}");
                Ok(())
            }
            result => result,
        }
    }
}

pub struct InNetworkLink {
    _source: String,
    stream: BufReader<NetworkStream>,
    senders: Senders,
    resources: Option<Arc<ExecutionResources>>,
    _io: Option<MemoryReservation>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MessageType {
    Data,
    Signal,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Header {
    src_operator: u32,
    src_subtask: u32,
    dst_operator: u32,
    dst_subtask: u32,
    len: usize,
    message_type: MessageType,
}

impl Header {
    fn from_quad(quad: Quad, len: usize, message_type: MessageType) -> Self {
        Self {
            src_operator: quad.src_id as u32,
            src_subtask: quad.src_idx as u32,
            dst_operator: quad.dst_id as u32,
            dst_subtask: quad.dst_idx as u32,
            len,
            message_type,
        }
    }

    fn as_quad(&self) -> Quad {
        Quad {
            src_id: self.src_operator as usize,
            src_idx: self.src_subtask as usize,
            dst_id: self.dst_operator as usize,
            dst_idx: self.dst_subtask as usize,
        }
    }

    fn from_bytes<B: Buf>(mut bytes: B) -> Header {
        Header {
            src_operator: bytes.get_u32_le(),
            src_subtask: bytes.get_u32_le(),
            dst_operator: bytes.get_u32_le(),
            dst_subtask: bytes.get_u32_le(),
            len: bytes.get_u32_le() as usize,
            message_type: match bytes.get_u32_le() {
                0 => MessageType::Data,
                1 => MessageType::Signal,
                b => panic!("invalid message type: {b}"),
            },
        }
    }

    async fn write<W: AsyncWrite + AsyncWriteExt>(
        &self,
        writer: &mut Pin<&mut W>,
    ) -> io::Result<()> {
        let mut bytes = [0u8; size_of::<Header>()];
        let mut buf = &mut bytes[..];
        buf.put_u32_le(self.src_operator);
        buf.put_u32_le(self.src_subtask);
        buf.put_u32_le(self.dst_operator);
        buf.put_u32_le(self.dst_subtask);
        buf.put_u32_le(self.len as u32);
        buf.put_u32_le(match self.message_type {
            MessageType::Data => 0,
            MessageType::Signal => 1,
        });

        writer.write_all(&bytes).await
    }
}

impl InNetworkLink {
    #[cfg(test)]
    fn new(
        source: String,
        stream: NetworkStream,
        senders: Senders,
        resources: Option<Arc<ExecutionResources>>,
    ) -> DataflowResult<Self> {
        let io = reserve_network(
            resources.as_deref(),
            "network input buffered I/O",
            16 * 1024,
        )?;
        Ok(Self::with_io(source, stream, senders, resources, io))
    }

    fn with_io(
        source: String,
        stream: NetworkStream,
        senders: Senders,
        resources: Option<Arc<ExecutionResources>>,
        io: Option<MemoryReservation>,
    ) -> Self {
        InNetworkLink {
            _source: source,
            stream: BufReader::new(stream),
            senders,
            resources,
            _io: io,
        }
    }

    async fn next(&mut self, header_buf: &mut [u8]) -> DataflowResult<()> {
        self.stream
            .read_exact(header_buf)
            .await
            .map_err(network_io_error)?;
        let header = Header::from_bytes(&header_buf[..]);
        let result = async {
            // The advertised wire length is charged before allocating or reading it.
            let _wire =
                reserve_network(self.resources.as_deref(), "network input frame", header.len)?;
            let mut buf = vec![0; header.len];
            self.stream
                .read_exact(&mut buf)
                .await
                .map_err(network_io_error)?;
            self.senders
                .send(header, buf, self.resources.as_deref())
                .await
        }
        .await;
        if let Err(error) = result {
            if let Some(sender) = self.senders.senders.get(&header.as_quad())
                && let Some(failure) = &sender.failure
            {
                failure.report(error).await;
                return Err(DataflowError::ExternalError(
                    "network input delivery failed; task failure reported".into(),
                ));
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn start(mut self) -> tokio::task::AbortHandle {
        tokio::spawn(async move {
            let mut header_buf = [0u8; size_of::<Header>()];
            loop {
                if let Err(error) = self.next(&mut header_buf).await {
                    warn!("Network input stopped: {error}");
                    break;
                }
            }
        })
        .abort_handle()
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Quad {
    pub src_id: usize,
    pub src_idx: usize,
    pub dst_id: usize,
    pub dst_idx: usize,
}

struct NetworkReceiver {
    quad: Quad,
    rx: BatchReceiver,
    dictionary_tracker: Arc<Mutex<DictionaryTracker>>,
    failure: Option<NetworkFailureReporter>,
}

struct OutNetworkLink {
    stream: BufWriter<NetworkStream>,
    receivers: Vec<NetworkReceiver>,
    resources: Option<Arc<ExecutionResources>>,
    _io: Option<MemoryReservation>,
}

impl OutNetworkLink {
    pub async fn connect(
        dest: &str,
        resources: Option<Arc<ExecutionResources>>,
    ) -> DataflowResult<Self> {
        let io = reserve_network(
            resources.as_deref(),
            "network output buffered I/O",
            16 * 1024,
        )?;
        let config = config();
        let mut rand = StdRng::from_os_rng();

        for i in 0..10 {
            match TcpStream::connect(&dest).await {
                Ok(tcp_stream) => {
                    let network_stream =
                        if let Some(tls) = config.get_tls_config(&config.worker.tls) {
                            match Self::connect_tls(tcp_stream, dest, tls).await {
                                Ok(tls_stream) => NetworkStream::TlsClient(tls_stream),
                                Err(e) => {
                                    warn!("Failed to establish TLS connection to {dest}: {:?}", e);
                                    tokio::time::sleep(Duration::from_millis(
                                        (i + 1) * (50 + rand.random_range(1..50)),
                                    ))
                                    .await;
                                    continue;
                                }
                            }
                        } else {
                            NetworkStream::Plain(tcp_stream)
                        };

                    return Ok(Self {
                        stream: BufWriter::new(network_stream),
                        receivers: vec![],
                        resources,
                        _io: io,
                    });
                }
                Err(e) => {
                    warn!("Failed to connect to {dest}: {:?}", e);
                    tokio::time::sleep(Duration::from_millis(
                        (i + 1) * (50 + rand.random_range(1..50)),
                    ))
                    .await;
                }
            }
        }
        Err(DataflowError::ExternalError(format!(
            "failed to connect to {dest}"
        )))
    }

    async fn connect_tls(
        tcp_stream: TcpStream,
        dest: &str,
        tls: &TlsConfig,
    ) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let client_config = arroyo_server_common::tls::create_tcp_client_tls_config(tls).await?;
        let connector = TlsConnector::from(client_config);

        let endpoint = Url::parse(&format!("tcp://{dest}")).context("invalid endpoint")?;

        let domain = match endpoint
            .host()
            .ok_or_else(|| anyhow!("could not get host from endpoint {}", dest))?
        {
            Host::Domain(s) => ServerName::try_from(intern(s))?,
            Host::Ipv4(ip) => ServerName::IpAddress(IpAddr::V4(ip.into())),
            Host::Ipv6(ip) => ServerName::IpAddress(IpAddr::V6(ip.into())),
        };

        let tls_stream = connector.connect(domain, tcp_stream).await?;
        Ok(tls_stream)
    }

    pub async fn add_receiver(
        &mut self,
        quad: Quad,
        rx: BatchReceiver,
        failure: Option<NetworkFailureReporter>,
    ) {
        self.receivers.push(NetworkReceiver {
            quad,
            rx,
            dictionary_tracker: Arc::new(Mutex::new(DictionaryTracker::new(true))),
            failure,
        });
    }

    pub fn start(mut self) -> tokio::task::AbortHandle {
        tokio::spawn(async move {
            let _io = self._io;
            let failures: Vec<_> = self.receivers.iter().filter_map(|receiver| receiver.failure.clone()).collect();
            let mut sel = InQReader::new();
            for NetworkReceiver {
                quad,
                mut rx,
                dictionary_tracker,
                failure,
            } in self.receivers
            {
                let resources = self.resources.clone();
                let stream = async_stream::stream! {
                    while let Some(item) = rx.recv().await {
                        // InQReader may retain a ready item while another socket write
                        // is blocked. Acquire the replacement charge before yielding.
                        let held = match &item {
                            ArrowMessage::Data(batch) => resources.as_ref().map(|r|
                                r.reserve_batch("network output input", batch)).transpose(),
                            ArrowMessage::Signal(_) => Ok(None),
                        };
                        yield (quad, dictionary_tracker.clone(), failure.clone(), item, held);
                    }
                };
                sel.push(Box::pin(stream));
            }
            let mut flush_interval: Interval = interval(Duration::from_millis(100));
            loop {
                select! {
                    next = sel.next() => {
                        let Some(((quad, dictionary_tracker, failure, msg, held), s)) = next else {
                            if let Err(error) = self.stream.flush().await {
                                if let Some(failure) = failures.first() { failure.report(network_io_error(error)).await; }
                                else { warn!("Network output final flush failed: {error}"); }
                            }
                            break;
                        };
                        let result: DataflowResult<()> = async {
                            let _held = held?;
                            match msg {
                                ArrowMessage::Signal(signal) => {
                                    let _encoded = reserve_network(self.resources.as_deref(), "network output signal", 256)?;
                                    let data = bincode::encode_to_vec(&signal, config::standard())
                                        .map_err(|error| DataflowError::ExternalError(error.to_string()))?;
                                    let header = Header::from_quad(quad, data.len(), MessageType::Signal);
                                    header.write(&mut Pin::new(&mut self.stream)).await.map_err(network_io_error)?;
                                    self.stream.write_all(&data).await.map_err(network_io_error)?;
                                }
                                ArrowMessage::Data(data) => {
                                    write_accounted_batch(&mut Pin::new(&mut self.stream), quad, &data, &dictionary_tracker, self.resources.as_deref()).await?;
                                }
                            }
                            Ok(())
                        }.await;
                        if let Err(error) = result {
                            if let Some(failure) = failure { failure.report(error).await; }
                            else { warn!("Network output failed: {error}"); }
                            break;
                        }
                        sel.push(s);
                    }
                    _ = flush_interval.tick() => {
                        if let Err(error) = self.stream.flush().await {
                            if let Some(failure) = failures.first() { failure.report(network_io_error(error)).await; }
                            else { warn!("Network output flush failed: {error}"); }
                            break;
                        }
                    }
                }
            }
        }).abort_handle()
    }
}

enum InStreamsOrSenders {
    InStreams(Vec<(NetworkStream, Option<MemoryReservation>)>),
    Senders(Senders),
}

pub struct NetworkManager {
    port: u16,
    in_streams: Arc<Mutex<InStreamsOrSenders>>,
    out_streams: Arc<Mutex<HashMap<Quad, OutNetworkLink>>>,
    tls_acceptor: Option<TlsAcceptor>,
    resources: Option<Arc<ExecutionResources>>,
    tasks: NetworkTasks,
}

impl NetworkManager {
    pub async fn new(port: u16) -> anyhow::Result<Self> {
        let config = config();
        let tls_acceptor = if config.is_tls_enabled(&config.worker.tls) {
            if let Some(tls_config) = config.get_tls_config(&config.worker.tls) {
                let server_config =
                    arroyo_server_common::tls::create_tcp_server_tls_config(tls_config).await?;
                Some(TlsAcceptor::from(Arc::new(server_config)))
            } else {
                None
            }
        } else {
            None
        };

        Ok(NetworkManager {
            port,
            in_streams: Arc::new(Mutex::new(InStreamsOrSenders::InStreams(vec![]))),
            out_streams: Arc::new(Mutex::new(HashMap::new())),
            tls_acceptor,
            resources: configured_execution_resources()?,
            tasks: NetworkTasks::default(),
        })
    }

    pub(crate) fn task_registry(&self) -> NetworkTasks {
        self.tasks.clone()
    }

    pub async fn open_listener(&mut self, shutdown_guard: ShutdownGuard) -> u16 {
        let socket_addr = SocketAddr::new(config().worker.bind_address, self.port);

        let listener = TcpListener::bind(socket_addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        info!(
            "Started worker data listener {} on {}",
            self.tls_acceptor
                .as_ref()
                .map(|_| "with TLS")
                .unwrap_or_default(),
            socket_addr
        );

        let streams = Arc::clone(&self.in_streams);
        let tls_acceptor = self.tls_acceptor.clone();
        let resources = self.resources.clone();
        let tasks = self.tasks.clone();

        let listener_task = shutdown_guard.into_spawn_task(async move {
            loop {
                let (tcp_stream, _) = listener.accept().await?;
                let ip = tcp_stream.local_addr().unwrap().to_string();
                let io = match reserve_network(
                    resources.as_deref(),
                    "network input buffered I/O",
                    16 * 1024,
                ) {
                    Ok(io) => io,
                    Err(error) => {
                        let failure = {
                            let streams = streams.lock().await;
                            match &*streams {
                                InStreamsOrSenders::Senders(senders) => senders
                                    .senders
                                    .values()
                                    .find_map(|sender| sender.failure.clone()),
                                _ => None,
                            }
                        };
                        if let Some(failure) = failure {
                            failure.report(error).await;
                        } else {
                            warn!("Network input admission failed: {error}");
                        }
                        continue;
                    }
                };

                let network_stream = if let Some(ref acceptor) = tls_acceptor {
                    match acceptor.accept(tcp_stream).await {
                        Ok(tls_stream) => NetworkStream::TlsServer(tls_stream),
                        Err(e) => {
                            warn!("Failed to establish TLS connection: {:?}", e);
                            continue;
                        }
                    }
                } else {
                    NetworkStream::Plain(tcp_stream)
                };

                let mut s = streams.lock().await;

                match &mut *s {
                    InStreamsOrSenders::InStreams(streams) => streams.push((network_stream, io)),
                    InStreamsOrSenders::Senders(senders) => {
                        let senders = senders.clone();
                        let resources = resources.clone();

                        tasks.register(
                            InNetworkLink::with_io(ip, network_stream, senders, resources, io)
                                .start(),
                        );
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        });

        self.tasks.register(listener_task.abort_handle());
        port
    }

    pub async fn start(&mut self, senders: Senders) {
        let mut sockets = self.in_streams.lock().await;

        match &mut *sockets {
            InStreamsOrSenders::InStreams(in_streams) => {
                for (s, io) in in_streams.drain(..) {
                    let senders = senders.clone();
                    let resources = self.resources.clone();
                    self.tasks.register(
                        InNetworkLink::with_io(
                            s.local_addr().unwrap().to_string(),
                            s,
                            senders,
                            resources,
                            io,
                        )
                        .start(),
                    );
                }
            }
            InStreamsOrSenders::Senders(_) => {
                panic!("already started!");
            }
        }

        *sockets = InStreamsOrSenders::Senders(senders.clone());

        let mut out_streams = self.out_streams.lock().await;
        for (_, s) in out_streams.drain() {
            self.tasks.register(s.start());
        }
    }

    pub async fn connect(&self, addr: &str, quad: Quad, rx: BatchReceiver) {
        self.connect_reported(addr, quad, rx, None).await;
    }

    pub(crate) async fn connect_reported(
        &self,
        addr: &str,
        quad: Quad,
        rx: BatchReceiver,
        failure: Option<NetworkFailureReporter>,
    ) {
        let link = match OutNetworkLink::connect(addr, self.resources.clone()).await {
            Ok(link) => link,
            Err(error) => {
                if let Some(failure) = failure {
                    // The control consumer starts after Engine::start finishes
                    // connecting links. Keep delivery awaited in an owned task so
                    // a full startup control queue cannot block that startup.
                    self.tasks.register(
                        tokio::spawn(async move { failure.report(error).await }).abort_handle(),
                    );
                } else {
                    warn!("Network connection failed: {error}");
                }
                return;
            }
        };
        let mut ins = self.out_streams.lock().await;
        if let std::collections::hash_map::Entry::Vacant(e) = ins.entry(quad) {
            e.insert(link);
        }

        ins.get_mut(&quad)
            .as_mut()
            .unwrap()
            .add_receiver(quad, rx, failure)
            .await;
    }
}

#[inline]
fn pad_to_8(len: u32) -> usize {
    (((len + 7) & !7) - len) as usize
}

async fn write_accounted_batch<W: AsyncWrite + AsyncWriteExt>(
    writer: &mut Pin<&mut W>,
    quad: Quad,
    batch: &RecordBatch,
    dictionary_tracker: &Mutex<DictionaryTracker>,
    resources: Option<&ExecutionResources>,
) -> DataflowResult<()> {
    let _encoding = reserve_encode(resources, batch)?;
    let (_, encoded_message) = {
        let mut dictionary_tracker = dictionary_tracker.lock().await;
        IpcDataGenerator {}.encoded_batch(
            batch,
            &mut dictionary_tracker,
            &IpcWriteOptions::default(),
        )?
    };
    write_message_and_header(writer, quad, encoded_message).await?;
    Ok(())
}

// Async-ified and modified version of arrow::ipc::writer::write_message
pub async fn write_message_and_header<W: AsyncWrite + AsyncWriteExt>(
    writer: &mut Pin<&mut W>,
    quad: Quad,
    encoded: EncodedData,
) -> Result<(), ArrowError> {
    let arrow_data_len = encoded.arrow_data.len();
    if !arrow_data_len.is_multiple_of(8) {
        return Err(ArrowError::MemoryError(
            "Arrow data not aligned".to_string(),
        ));
    }

    let buffer = encoded.ipc_message;

    let prefix_size = 4usize;
    let flatbuf_size = buffer.len();

    let total_size = prefix_size
        .checked_add(flatbuf_size)
        .and_then(|size| size.checked_add(arrow_data_len))
        .filter(|size| u32::try_from(*size).is_ok())
        .ok_or_else(|| {
            ArrowError::MemoryError("network IPC frame exceeds u32 wire length".into())
        })?;

    let header = Header::from_quad(quad, total_size, MessageType::Data);
    header.write(writer).await?;

    let mut bytes_written = 0;

    // write the flatbuf
    if flatbuf_size > 0 {
        writer
            .write_all(&(flatbuf_size as u32).to_le_bytes())
            .await?;
        writer.write_all(&buffer).await?;
        bytes_written += buffer.len() + 4;
    }
    // write arrow data
    if arrow_data_len > 0 {
        let len = encoded.arrow_data.len() as u32;
        let pad_len = pad_to_8(len);

        // write body buffer
        writer.write_all(&encoded.arrow_data).await?;
        bytes_written += encoded.arrow_data.len();
        if pad_len > 0 {
            writer.write_all(&vec![0u8; pad_len][..]).await?;
            bytes_written += pad_len;
        }
    }

    assert_eq!(
        bytes_written, total_size,
        "Wrote unexpected number of bytes {bytes_written} != {total_size}"
    );

    Ok(())
}

fn read_message(schema: SchemaRef, data: Vec<u8>) -> anyhow::Result<RecordBatch> {
    // Borrow the already-accounted frame metadata rather than allocating an
    // attacker-advertised second metadata Vec.
    let (metadata, body) = ipc_parts(&data)?;
    let message = arrow::ipc::root_as_message(metadata)
        .map_err(|error| anyhow!("Unable to read IPC message: {error}"))?;
    let arrow::ipc::MessageHeader::RecordBatch = message.header_type() else {
        bail!("unexpected message type: {:?}", message.header_type());
    };
    let batch = message
        .header_as_record_batch()
        .context("missing IPC record batch")?;
    let body_len = usize::try_from(message.bodyLength()).context("negative IPC body length")?;
    anyhow::ensure!(body.len() == body_len, "IPC body length mismatch");
    let mut batch_buf = MutableBuffer::from_len_zeroed(body_len);
    batch_buf.copy_from_slice(body);
    Ok(read_record_batch(
        &batch_buf.into(),
        batch,
        schema,
        &HashMap::new(),
        None,
        &message.version(),
    )?)
}

#[cfg(test)]
mod test {
    use arrow_array::{ArrayRef, RecordBatch, TimestampNanosecondArray, UInt64Array};
    use arrow_schema::{Field, Schema, TimeUnit};
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::{pin::Pin, time::Duration};

    use arroyo_operator::context::batch_bounded;
    use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
    use arroyo_types::{ArrowMessage, CheckpointBarrier, SignalMessage, to_nanos};
    use tokio::time::timeout;

    use crate::network_manager::{MessageType, Quad};

    use super::{Header, NetworkManager, Senders};

    fn resources(memory_bytes: usize) -> Arc<super::ExecutionResources> {
        Arc::new(
            super::ExecutionResources::new(arroyo_rpc::config::ExecutionResourceConfig {
                memory_bytes,
                max_batch_bytes: memory_bytes,
            })
            .unwrap(),
        )
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap()
    }

    fn quad() -> Quad {
        Quad {
            src_id: 1,
            src_idx: 0,
            dst_id: 2,
            dst_idx: 0,
        }
    }

    fn encoded(batch: &RecordBatch) -> Vec<u8> {
        let (_, encoded) = arrow::ipc::writer::IpcDataGenerator {}
            .encoded_batch(
                batch,
                &mut arrow::ipc::writer::DictionaryTracker::new(true),
                &arrow::ipc::writer::IpcWriteOptions::default(),
            )
            .unwrap();
        let mut bytes = (encoded.ipc_message.len() as u32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&encoded.ipc_message);
        bytes.extend_from_slice(&encoded.arrow_data);
        bytes
    }

    #[tokio::test]
    async fn network_setup_failure_does_not_wait_for_startup_control_consumer() {
        let resources = resources(1024);
        let mut network = NetworkManager::new(0).await.unwrap();
        network.resources = Some(resources.clone());
        let (control, mut responses) = tokio::sync::mpsc::channel(128);
        for task_id in 0..128 {
            control
                .send(arroyo_rpc::ControlResp::TaskStarted {
                    task_id,
                    subtask_idx: 0,
                    start_time: SystemTime::now(),
                })
                .await
                .unwrap();
        }
        let (_tx, rx) = batch_bounded(1);
        // The configured budget cannot admit buffered network I/O. No TCP
        // attempt or control receiver is needed to reproduce the startup cycle.
        timeout(
            Duration::from_secs(1),
            network.connect_reported(
                "127.0.0.1:1",
                quad(),
                rx,
                Some(super::NetworkFailureReporter::new(control.clone(), 72, 3)),
            ),
        )
        .await
        .expect("link setup must finish before the control consumer starts");
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);

        // Starting the consumer must deliver every queued event and the typed
        // failure exactly once, including its originating task identity.
        for expected_task_id in 0..128 {
            let Some(arroyo_rpc::ControlResp::TaskStarted { task_id, .. }) = responses.recv().await
            else {
                panic!("expected queued startup event");
            };
            assert_eq!(task_id, expected_task_id);
        }
        let Some(arroyo_rpc::ControlResp::TaskFailed {
            task_id,
            subtask_idx,
            error,
        }) = timeout(Duration::from_secs(1), responses.recv())
            .await
            .unwrap()
        else {
            panic!("expected deferred setup failure");
        };
        assert_eq!((task_id, subtask_idx), (72, 3));
        assert!(error.message.contains("network output buffered I/O"));
        assert_eq!(error.domain, arroyo_rpc::errors::ErrorDomain::External);
        assert_eq!(error.retry_hint, arroyo_rpc::errors::RetryHint::WithBackoff);
        drop(control);
        assert!(
            timeout(Duration::from_secs(1), responses.recv())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn network_setup_failure_report_is_cancelled_with_engine_network_tasks() {
        let mut network = NetworkManager::new(0).await.unwrap();
        network.resources = Some(resources(1024));
        let (control, mut responses) = tokio::sync::mpsc::channel(1);
        control
            .send(arroyo_rpc::ControlResp::TaskStarted {
                task_id: 1,
                subtask_idx: 0,
                start_time: SystemTime::now(),
            })
            .await
            .unwrap();
        let tasks = network.task_registry();
        let (_tx, rx) = batch_bounded(1);
        timeout(
            Duration::from_secs(1),
            network.connect_reported(
                "127.0.0.1:1",
                quad(),
                rx,
                Some(super::NetworkFailureReporter::new(control.clone(), 72, 3)),
            ),
        )
        .await
        .unwrap();
        tokio::task::yield_now().await;
        tasks.abort();
        // Cancellation must drop the pending report's sender even while the
        // queue remains full; otherwise engine shutdown leaks the control loop.
        drop(control);
        timeout(Duration::from_secs(1), async {
            while !responses.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("engine cancellation must release the pending report sender");
        assert!(matches!(
            responses.recv().await,
            Some(arroyo_rpc::ControlResp::TaskStarted { task_id: 1, .. })
        ));
        assert!(responses.recv().await.is_none());
    }

    #[tokio::test]
    async fn network_frame_limit_fails_before_waiting_for_body_and_reports_task() {
        let resources = resources(32 * 1024);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut writer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (reader, _) = listener.accept().await.unwrap();
        let (tx, _rx) = batch_bounded(1);
        let (control, mut failures) = tokio::sync::mpsc::channel(1);
        let mut senders = Senders::new();
        senders.add_reported(
            quad(),
            batch().schema(),
            tx,
            super::NetworkFailureReporter::new(control, 72, 3),
        );
        let mut link = super::InNetworkLink::new(
            "test".into(),
            super::NetworkStream::Plain(reader),
            senders,
            Some(resources.clone()),
        )
        .unwrap();
        Header::from_quad(quad(), 64 * 1024, MessageType::Data)
            .write(&mut Pin::new(&mut writer))
            .await
            .unwrap();
        // Deliberately send no body: admission must fail before read_exact.
        let result = timeout(
            Duration::from_secs(1),
            link.next(&mut [0; std::mem::size_of::<Header>()]),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        let failure = failures.recv().await.unwrap();
        let arroyo_rpc::ControlResp::TaskFailed {
            task_id,
            subtask_idx,
            error,
        } = failure
        else {
            panic!("expected task failure");
        };
        assert_eq!((task_id, subtask_idx), (72, 3));
        assert!(
            error.message.contains("network input frame"),
            "{}",
            error.message
        );
        assert_eq!(resources.runtime.memory_pool.reserved(), 16 * 1024 + 256);
        drop(link);
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn network_decoded_delivery_retains_budget_until_cancelled() {
        let resources = resources(1024 * 1024);
        let batch = batch();
        let data = encoded(&batch);
        let (tx, mut rx) = batch_bounded(1);
        tx.send(ArrowMessage::Signal(SignalMessage::Watermark(
            arroyo_types::Watermark::Idle,
        )))
        .await
        .unwrap();
        let mut senders = Senders::new();
        senders.add(quad(), batch.schema(), tx);
        let header = Header::from_quad(quad(), data.len(), MessageType::Data);
        {
            let sending = senders.send(header, data, Some(&resources));
            tokio::pin!(sending);
            assert!(
                timeout(Duration::from_millis(20), &mut sending)
                    .await
                    .is_err()
            );
            assert!(resources.runtime.memory_pool.reserved() > batch.get_array_memory_size());
        }
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        assert!(matches!(rx.recv().await, Some(ArrowMessage::Signal(_))));
        assert!(timeout(Duration::from_millis(20), rx.recv()).await.is_err());
    }

    #[test]
    fn network_decode_admission_rejects_truncated_frame_without_allocation() {
        let resources = resources(1024 * 1024);
        let data = u32::MAX.to_le_bytes().to_vec();
        assert!(super::reserve_decode(Some(&resources), &batch().schema(), &data).is_err());
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn network_slow_writer_retains_encoding_budget_until_cancelled() {
        let resources = resources(1024 * 1024);
        let batch = batch();
        let dictionary = tokio::sync::Mutex::new(arrow::ipc::writer::DictionaryTracker::new(true));
        let (mut writer, _slow_reader) = tokio::io::duplex(8);
        let mut writer = Pin::new(&mut writer);
        {
            let writing = super::write_accounted_batch(
                &mut writer,
                quad(),
                &batch,
                &dictionary,
                Some(&resources),
            );
            tokio::pin!(writing);
            assert!(
                timeout(Duration::from_millis(20), &mut writing)
                    .await
                    .is_err()
            );
            assert!(resources.runtime.memory_pool.reserved() > batch.get_array_memory_size());
        }
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }

    #[test]
    fn network_codec_admits_nullable_struct_and_releases_successful_scopes() {
        use arrow_array::{NullArray, StructArray};
        let fields = arrow_schema::Fields::from(vec![
            Field::new("empty", arrow_schema::DataType::Null, true),
            Field::new("value", arrow_schema::DataType::UInt64, true),
        ]);
        let value = StructArray::new(
            fields,
            vec![
                Arc::new(NullArray::new(3)),
                Arc::new(UInt64Array::from(vec![Some(7), None, Some(11)])),
            ],
            None,
        );
        let batch =
            RecordBatch::try_from_iter(vec![("record", Arc::new(value) as ArrayRef)]).unwrap();
        let resources = resources(1024 * 1024);
        let encoding = super::reserve_encode(Some(&resources), &batch).unwrap();
        let data = encoded(&batch);
        let decoding = super::reserve_decode(Some(&resources), &batch.schema(), &data).unwrap();
        let result = super::read_message(batch.schema(), data).unwrap();
        assert_eq!(result, batch);
        assert!(resources.runtime.memory_pool.reserved() > result.get_array_memory_size());
        drop((encoding, decoding));
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn network_task_abort_releases_permits_and_cancels_late_registration() {
        let resources = resources(1024);
        let tasks = super::NetworkTasks::default();
        let held = resources.reserve_bytes("test network task", 512).unwrap();
        let task = tokio::spawn(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        tasks.register(task.abort_handle());
        tasks.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
        let held = resources.reserve_bytes("late network task", 512).unwrap();
        let late = tokio::spawn(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        tasks.register(late.abort_handle());
        assert!(late.await.unwrap_err().is_cancelled());
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn network_output_exits_and_releases_io_budget_when_inputs_close() {
        let resources = resources(1024 * 1024);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (_peer, _) = listener.accept().await.unwrap();
        let io = super::reserve_network(Some(&resources), "network output buffered I/O", 16 * 1024)
            .unwrap();
        let mut link = super::OutNetworkLink {
            stream: tokio::io::BufWriter::new(super::NetworkStream::Plain(stream)),
            receivers: vec![],
            resources: Some(resources.clone()),
            _io: io,
        };
        let (tx, rx) = batch_bounded(1);
        link.add_receiver(quad(), rx, None).await;
        let task = link.start();
        drop(tx);
        timeout(Duration::from_secs(1), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(resources.runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn test_header_serdes() {
        let mut buffer = vec![];

        let header = Header {
            src_operator: 12412,
            src_subtask: 3,
            dst_operator: 9098,
            dst_subtask: 100,
            len: 30,
            message_type: MessageType::Signal,
        };

        header.write(&mut Pin::new(&mut buffer)).await.unwrap();

        let h2 = Header::from_bytes(&buffer[..]);

        assert_eq!(header, h2);
    }

    #[tokio::test]
    async fn test_client_server() {
        let (server_tx, mut server_rx) = batch_bounded(10);

        let mut senders = Senders::new();

        let quad = Quad {
            src_id: 50,
            src_idx: 1,
            dst_id: 21234,
            dst_idx: 3,
        };

        let time = SystemTime::now();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow_schema::DataType::UInt64, false),
            Field::new(
                "time",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from((0..10).collect::<Vec<_>>())),
            Arc::new(TimestampNanosecondArray::from(vec![
                to_nanos(time) as i64;
                10
            ])),
        ];

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        senders.add(quad, schema.clone(), server_tx);

        let shutdown = Shutdown::new("test", SignalBehavior::None);
        let mut nm = NetworkManager::new(0).await.unwrap();
        let port = nm.open_listener(shutdown.guard("test")).await;

        let (client_tx, client_rx) = batch_bounded(10);
        nm.connect(&format!("127.0.0.1:{port}"), quad, client_rx)
            .await;

        nm.start(senders).await;

        client_tx
            .send(ArrowMessage::Data(batch.clone()))
            .await
            .unwrap();

        let result = timeout(Duration::from_secs(1), server_rx.recv())
            .await
            .unwrap()
            .expect("timed out");

        let ArrowMessage::Data(result) = result else {
            panic!("expected bytes");
        };

        assert_eq!(result, batch);

        // test control message
        let message = ArrowMessage::Signal(SignalMessage::Barrier(CheckpointBarrier {
            epoch: 5,
            min_epoch: 3,
            timestamp: SystemTime::now(),
            then_stop: false,
        }));

        client_tx.send(message.clone()).await.unwrap();

        let result = timeout(Duration::from_secs(1), server_rx.recv())
            .await
            .unwrap()
            .expect("timed out");

        assert_eq!(result, message);

        // Exercise the actual outbound encoder and inbound signal decoder,
        // including progress before the epoch and a partition becoming idle.
        for watermark in [
            arroyo_types::Watermark::EventTime(SystemTime::UNIX_EPOCH - Duration::from_secs(2)),
            arroyo_types::Watermark::EventTime(SystemTime::UNIX_EPOCH - Duration::from_nanos(1)),
            arroyo_types::Watermark::Idle,
            arroyo_types::Watermark::EventTime(SystemTime::UNIX_EPOCH + Duration::from_secs(2)),
        ] {
            let message = ArrowMessage::Signal(SignalMessage::Watermark(watermark));
            client_tx.send(message.clone()).await.unwrap();
            let received = timeout(Duration::from_secs(1), server_rx.recv())
                .await
                .unwrap()
                .expect("network signal channel closed");
            assert_eq!(received, message);
        }
    }
}
