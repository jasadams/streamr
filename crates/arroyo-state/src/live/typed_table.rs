//! Schema-aware keyed rows over the live-state contract. An owner holds a working
//! scope across related reads and mutations, then commits one ordered batch.
use super::{
    LiveStateBackend, LiveStateError, Ownership, ReadOptions, Result, ScanCursor, ScanRange,
    ScanRequest, StateKey, StateNamespace, StateSnapshot,
    resources::{ResourcePermit, WorkerStateResources},
    write::AdmittedWriteBatch,
};
use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    row::{RowConverter, SortField},
};
use arrow_array::{BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, SchemaRef};
use std::{
    collections::BTreeMap,
    io::{self, Cursor, Write},
    sync::Arc,
};
use tokio::sync::{Mutex, MutexGuard};

pub const ENCODING_VERSION: u32 = 1;
const MAGIC: &[u8; 8] = b"STRTBL01";

/// Supplied by the caller's catalog. Identity is persisted in the namespace and
/// row header; reopen with an incompatible schema never silently reads old rows.
#[derive(Clone, Debug)]
pub struct TableDescriptor {
    pub table_identity: Vec<u8>,
    pub schema_identity: Vec<u8>,
    pub schema: SchemaRef,
    pub primary_key: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableLimits {
    pub key_bytes: usize,
    pub row_bytes: usize,
    pub decoded_bytes: usize,
    pub scope_bytes: usize,
    pub scope_operations: usize,
    pub page_bytes: usize,
    pub page_entries: usize,
}

struct EncodedTableKey {
    key: StateKey,
    _permit: ResourcePermit,
}

pub struct TypedTable {
    backend: Arc<dyn LiveStateBackend>,
    namespace: StateNamespace,
    descriptor: TableDescriptor,
    limits: TableLimits,
    resources: WorkerStateResources,
    owner: Arc<Mutex<()>>,
}

/// Retains the decoded-value reservation until the caller drops the row. Input
/// batches are caller-owned and must be bounded by the operator before arrival.
pub struct TableRow {
    batch: RecordBatch,
    _permit: Arc<ResourcePermit>,
}

impl TableRow {
    /// Borrowed access keeps the decoded allocation charged to this row.
    /// Deliberately cloning Arrow buffers requires separate caller admission.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

/// Private overlay owns only bounded encoded values. Serial event steps may
/// share a bounded input-batch scope; dropping it discards pending mutations.
pub struct WorkingScope<'a> {
    table: &'a TypedTable,
    _owner: MutexGuard<'a, ()>,
    admitted: AdmittedWriteBatch,
    overlay: BTreeMap<StateKey, Option<Vec<u8>>>,
    _overlay_permit: ResourcePermit,
}

pub struct TableSnapshot {
    snapshot: StateSnapshot,
    namespace: StateNamespace,
    descriptor: TableDescriptor,
    limits: TableLimits,
    resources: WorkerStateResources,
}
pub struct TablePage {
    pub rows: Vec<TableRow>,
    pub next_cursor: Option<ScanCursor>,
    _page_permit: ResourcePermit,
}

fn invalid(message: impl Into<String>) -> LiveStateError {
    LiveStateError::InvalidEncoding(message.into())
}
fn bound(required: usize, limit: usize) -> Result<()> {
    if required > limit {
        Err(LiveStateError::ReadLimitExceeded { required, limit })
    } else {
        Ok(())
    }
}
fn key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}
fn row_type(data_type: &DataType) -> bool {
    key_type(data_type)
        || matches!(
            data_type,
            DataType::Float32 | DataType::Float64 | DataType::Date64
        )
}

impl TypedTable {
    pub fn new(
        backend: Arc<dyn LiveStateBackend>,
        namespace: StateNamespace,
        descriptor: TableDescriptor,
        limits: TableLimits,
        resources: WorkerStateResources,
    ) -> Result<Self> {
        if [
            limits.key_bytes,
            limits.row_bytes,
            limits.decoded_bytes,
            limits.scope_bytes,
            limits.scope_operations,
            limits.page_bytes,
            limits.page_entries,
        ]
        .contains(&0)
        {
            return Err(LiveStateError::InvalidLimit);
        }
        // Initial ownership matches the catalog's fixed single-owner contract.
        if !matches!(
            namespace.ownership,
            Ownership::PartitionLocal {
                subtask: 0,
                parallelism: 1
            }
        ) {
            return Err(invalid(
                "typed tables currently require partition-local parallelism 1",
            ));
        }
        super::encoding::encoded_namespace_size(&namespace)?;
        if descriptor.table_identity.is_empty()
            || descriptor.schema_identity.is_empty()
            || descriptor.primary_key.is_empty()
        {
            return Err(invalid(
                "typed table requires table/schema identities and a primary key",
            ));
        }
        if namespace.table != descriptor.table_identity {
            return Err(invalid("namespace must use the stable table identity"));
        }
        let identity_bytes = descriptor
            .table_identity
            .len()
            .checked_add(descriptor.schema_identity.len())
            .ok_or(LiveStateError::InvalidLimit)?;
        bound(identity_bytes, limits.key_bytes)?;
        bound(identity_bytes, limits.decoded_bytes)?;
        bound(
            schema_owned_bytes(&descriptor.schema)?,
            limits.decoded_bytes,
        )?;
        let mut seen = std::collections::BTreeSet::new();
        for index in &descriptor.primary_key {
            let field = descriptor
                .schema
                .fields()
                .get(*index)
                .ok_or_else(|| invalid("primary key index out of range"))?;
            if !seen.insert(*index)
                || field.is_nullable()
                || !key_type(field.data_type())
                || field.metadata().contains_key("ARROW:extension:name")
            {
                return Err(invalid(
                    "primary key requires distinct non-null supported columns",
                ));
            }
        }
        for field in descriptor.schema.fields() {
            if !row_type(field.data_type()) || field.metadata().contains_key("ARROW:extension:name")
            {
                return Err(invalid(format!(
                    "unsupported state-table type {}",
                    field.data_type()
                )));
            }
        }
        validate_headroom(limits, &namespace, &descriptor, &resources)?;
        // Reuse the registered operator/table namespace. Schema identity belongs
        // in the value envelope so incompatible reopen surfaces an error.
        Ok(Self {
            backend,
            namespace,
            descriptor,
            limits,
            resources,
            owner: Arc::new(Mutex::new(())),
        })
    }
    pub(crate) fn with_owner(mut self, owner: Arc<Mutex<()>>) -> Self {
        self.owner = owner;
        self
    }
    pub fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }
    /// Register/export this namespace exactly once with the checkpoint owner.
    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }
    pub async fn begin(&self) -> Result<WorkingScope<'_>> {
        let owner = self.owner.lock().await;
        // Reserve both copies before encoding or cloning any pending values.
        let overlay = self
            .resources
            .try_decoded_value(scope_reservation(self.limits)?)?;
        let admitted = self
            .backend
            .try_admit_write(self.limits.scope_bytes, self.limits.scope_operations)?;
        Ok(WorkingScope {
            table: self,
            _owner: owner,
            admitted,
            overlay: BTreeMap::new(),
            _overlay_permit: overlay,
        })
    }
    pub async fn get(&self, key: &RecordBatch) -> Result<Option<TableRow>> {
        let _owner = self.owner.lock().await;
        let permit = Arc::new(
            self.resources.try_decoded_value(
                self.limits
                    .decoded_bytes
                    .checked_mul(3)
                    .ok_or(LiveStateError::InvalidLimit)?,
            )?,
        );
        let key = self.key(key)?;
        self.backend
            .try_get(
                &key.key,
                ReadOptions {
                    max_bytes: self.limits.row_bytes.min(self.limits.decoded_bytes),
                },
            )
            .await?
            .map(|bytes| decode(&bytes, &self.descriptor, self.limits, permit))
            .transpose()
    }
    pub async fn snapshot(&self) -> Result<TableSnapshot> {
        let _owner = self.owner.lock().await;
        Ok(TableSnapshot {
            snapshot: self.backend.snapshot().await?,
            namespace: self.namespace.clone(),
            descriptor: self.descriptor.clone(),
            limits: self.limits,
            resources: self.resources.clone(),
        })
    }
    fn key(&self, input: &RecordBatch) -> Result<EncodedTableKey> {
        encode_key(
            input,
            &self.descriptor,
            &self.namespace,
            self.limits,
            &self.resources,
        )
    }
    fn row_key(&self, row: &RecordBatch) -> Result<EncodedTableKey> {
        validate_row(row, &self.descriptor, self.limits)?;
        self.key(
            &row.project(&self.descriptor.primary_key)
                .map_err(|e| invalid(e.to_string()))?,
        )
    }
}
impl WorkingScope<'_> {
    fn check_table(&self, table: &TypedTable) -> Result<()> {
        if !Arc::ptr_eq(&self.table.backend, &table.backend)
            || !Arc::ptr_eq(&self.table.owner, &table.owner)
            || !self.table.resources.same_pool(&table.resources)
            || self.table.limits != table.limits
        {
            return Err(invalid(
                "working scope requires tables from the same owner, backend and limits",
            ));
        }
        Ok(())
    }
    pub async fn get(&self, key: &RecordBatch) -> Result<Option<TableRow>> {
        self.get_from(self.table, key).await
    }
    pub async fn get_from(
        &self,
        table: &TypedTable,
        key: &RecordBatch,
    ) -> Result<Option<TableRow>> {
        self.check_table(table)?;
        let permit = Arc::new(
            table.resources.try_decoded_value(
                table
                    .limits
                    .decoded_bytes
                    .checked_mul(3)
                    .ok_or(LiveStateError::InvalidLimit)?,
            )?,
        );
        let key = table.key(key)?;
        if let Some(value) = self.overlay.get(&key.key) {
            return value
                .as_ref()
                .map(|bytes| decode(bytes, &table.descriptor, table.limits, permit))
                .transpose();
        }
        table
            .backend
            .try_get(
                &key.key,
                ReadOptions {
                    max_bytes: table.limits.row_bytes.min(table.limits.decoded_bytes),
                },
            )
            .await?
            .map(|bytes| decode(&bytes, &table.descriptor, table.limits, permit))
            .transpose()
    }
    pub fn put(&mut self, row: &RecordBatch) -> Result<()> {
        self.put_into(self.table, row)
    }
    pub fn put_into(&mut self, table: &TypedTable, row: &RecordBatch) -> Result<()> {
        self.check_table(table)?;
        let key = table.row_key(row)?;
        let value = encode(
            row,
            &table.descriptor,
            table.limits.row_bytes.min(table.limits.decoded_bytes),
        )?;
        self.admitted.put(&key.key, &value)?;
        self.overlay.insert(key.key, Some(value));
        Ok(())
    }
    pub fn delete(&mut self, key: &RecordBatch) -> Result<()> {
        self.delete_from(self.table, key)
    }
    pub fn delete_from(&mut self, table: &TypedTable, key: &RecordBatch) -> Result<()> {
        self.check_table(table)?;
        let key = table.key(key)?;
        self.admitted.delete(&key.key)?;
        self.overlay.insert(key.key, None);
        Ok(())
    }
    pub async fn commit(self) -> Result<()> {
        self.table.backend.write_admitted(self.admitted).await
    }
}
impl TableSnapshot {
    pub async fn get(&self, key: &RecordBatch) -> Result<Option<TableRow>> {
        let permit = Arc::new(
            self.resources.try_decoded_value(
                self.limits
                    .decoded_bytes
                    .checked_mul(3)
                    .ok_or(LiveStateError::InvalidLimit)?,
            )?,
        );
        let key = encode_key(
            key,
            &self.descriptor,
            &self.namespace,
            self.limits,
            &self.resources,
        )?;
        self.snapshot
            .try_get(
                &key.key,
                ReadOptions {
                    max_bytes: self.limits.row_bytes.min(self.limits.decoded_bytes),
                },
            )
            .await?
            .map(|bytes| decode(&bytes, &self.descriptor, self.limits, permit))
            .transpose()
    }
    pub async fn scan(&self, cursor: Option<ScanCursor>) -> Result<TablePage> {
        let permit = self.resources.try_scan_page(
            self.limits
                .page_bytes
                .checked_mul(2)
                .ok_or(LiveStateError::InvalidLimit)?,
        )?;
        let page = self
            .snapshot
            .try_scan(ScanRequest {
                range: ScanRange {
                    namespace: self.namespace.clone(),
                    prefix: None,
                    start: None,
                    end: None,
                },
                max_entries: self.limits.page_entries,
                max_bytes: self.limits.page_bytes,
                cursor,
            })
            .await?;
        let decoded_bytes = page
            .entries
            .len()
            .checked_mul(self.limits.decoded_bytes)
            .and_then(|bytes| bytes.checked_mul(3))
            .ok_or(LiveStateError::InvalidLimit)?;
        let decoded = Arc::new(self.resources.try_decoded_value(decoded_bytes)?);
        let mut rows = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            rows.push(decode(
                &entry.value,
                &self.descriptor,
                self.limits,
                decoded.clone(),
            )?);
        }
        Ok(TablePage {
            rows,
            next_cursor: page.next_cursor,
            _page_permit: permit,
        })
    }
}
/// A conservative converter bound for one row of supported primitive columns.
/// Arrow's variable encoding is at most 4 + ceil(n/32)*33 bytes (including
/// miniblock/null sentinels). Fixed-width columns need at most width + 1.
fn key_encoded_bound(input: &RecordBatch) -> Result<usize> {
    let mut bytes = 0usize;
    for column in input.columns() {
        let next = match column.data_type() {
            DataType::Utf8 => {
                let column = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("invalid Utf8 key array"))?;
                variable_bound(column.value(0).len())?
            }
            DataType::Binary => {
                let column = column
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| invalid("invalid Binary key array"))?;
                variable_bound(column.value(0).len())?
            }
            DataType::Boolean => 2,
            data_type => data_type
                .primitive_width()
                .ok_or_else(|| invalid("unsupported key type"))?
                .checked_add(1)
                .ok_or(LiveStateError::InvalidLimit)?,
        };
        bytes = bytes
            .checked_add(next)
            .ok_or(LiveStateError::InvalidLimit)?;
    }
    Ok(bytes)
}
fn variable_bound(bytes: usize) -> Result<usize> {
    bytes
        .checked_add(31)
        .map(|bytes| bytes / 32)
        .and_then(|blocks| blocks.checked_mul(33))
        .and_then(|bytes| bytes.checked_add(4))
        .ok_or(LiveStateError::InvalidLimit)
}
fn key_workspace(encoded: usize, columns: usize, namespace: &StateNamespace) -> Result<usize> {
    // Four encoded lengths conservatively cover the converter buffer, final
    // exact-capacity prefixed key and bounded allocator rounding. Column slots
    // cover SortField/encoding vectors, row offsets and fixed converter objects.
    encoded
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(namespace.table.len()))
        .and_then(|bytes| bytes.checked_add(columns.checked_mul(1024)?))
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or(LiveStateError::InvalidLimit)
}
fn encode_key(
    input: &RecordBatch,
    descriptor: &TableDescriptor,
    namespace: &StateNamespace,
    limits: TableLimits,
    resources: &WorkerStateResources,
) -> Result<EncodedTableKey> {
    // Borrow the projected schema: Schema::project would clone metadata strings.
    let schema = input.schema();
    if input.num_rows() != 1
        || schema.fields().len() != descriptor.primary_key.len()
        || schema.metadata() != descriptor.schema.metadata()
        || schema
            .fields()
            .iter()
            .zip(&descriptor.primary_key)
            .any(|(field, index)| field != &descriptor.schema.fields()[*index])
    {
        return Err(invalid(
            "key must contain exactly one row of the primary-key schema",
        ));
    }
    bound(input.get_array_memory_size(), limits.decoded_bytes)?;
    if input
        .columns()
        .iter()
        .any(|column| column.null_count() != 0)
    {
        return Err(invalid("null primary key"));
    }
    let encoded = key_encoded_bound(input)?;
    // Reject impossible keys before converter allocation. The exact complete
    // escaped state-key limit is checked after conversion but before copying.
    bound(
        encoded.checked_add(1).ok_or(LiveStateError::InvalidLimit)?,
        limits.key_bytes,
    )?;
    let permit =
        resources.try_decoded_value(key_workspace(encoded, input.num_columns(), namespace)?)?;
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let rows = converter
        .convert_columns(input.columns())
        .map_err(|error| invalid(error.to_string()))?;
    let bytes = rows.row(0);
    bound(bytes.as_ref().len(), encoded)?;
    let size = super::encoding::encoded_namespace_size(namespace)?
        .checked_add(1)
        .and_then(|size| size.checked_add(bytes.as_ref().len()))
        .and_then(|size| size.checked_add(bytes.as_ref().iter().filter(|byte| **byte == 0).count()))
        .and_then(|size| size.checked_add(2))
        .ok_or(LiveStateError::InvalidLimit)?;
    bound(size, limits.key_bytes)?;
    let mut key = Vec::with_capacity(bytes.as_ref().len() + 1);
    key.push(ENCODING_VERSION as u8);
    key.extend_from_slice(bytes.as_ref());
    Ok(EncodedTableKey {
        key: StateKey {
            namespace: namespace.clone(),
            key,
            routing_hash: None,
        },
        _permit: permit,
    })
}

fn scope_reservation(limits: TableLimits) -> Result<usize> {
    limits
        .scope_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(limits.decoded_bytes.checked_mul(3)?))
        .and_then(|bytes| {
            bytes.checked_add(
                limits
                    .scope_operations
                    .checked_mul(128 + std::mem::size_of::<StateKey>())?,
            )
        })
        .ok_or(LiveStateError::InvalidLimit)
}
fn validate_headroom(
    limits: TableLimits,
    namespace: &StateNamespace,
    descriptor: &TableDescriptor,
    resources: &WorkerStateResources,
) -> Result<()> {
    let config = resources.config();
    let read = limits
        .decoded_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(limits.row_bytes.min(limits.decoded_bytes)))
        .and_then(|bytes| bytes.checked_add(limits.key_bytes.checked_mul(2)?))
        .and_then(|bytes| {
            bytes.checked_add(
                std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<Vec<u8>>>(),
            )
        })
        .ok_or(LiveStateError::InvalidLimit)?;
    let decoded = scope_reservation(limits)?
        .checked_add(key_workspace(
            limits.key_bytes,
            descriptor.primary_key.len(),
            namespace,
        )?)
        .and_then(|bytes| bytes.checked_add(read))
        .ok_or(LiveStateError::InvalidLimit)?;
    bound(decoded, config.decoded_value_bytes)?;
    let page_decoded = limits
        .page_entries
        .checked_mul(limits.decoded_bytes)
        .and_then(|bytes| bytes.checked_mul(3))
        .ok_or(LiveStateError::InvalidLimit)?;
    bound(page_decoded, config.decoded_value_bytes)?;
    let namespace_bytes = super::encoding::encoded_namespace_size(namespace)?;
    let scan = limits
        .page_bytes
        .checked_mul(6)
        .and_then(|bytes| bytes.checked_add(namespace_bytes.checked_mul(20)?))
        .and_then(|bytes| bytes.checked_add(limits.key_bytes.checked_mul(4)?))
        .and_then(|bytes| {
            bytes.checked_add(
                limits
                    .page_entries
                    .checked_mul(std::mem::size_of::<super::ScanEntry>() * 2)?,
            )
        })
        .ok_or(LiveStateError::InvalidLimit)?;
    bound(scan, config.scan_page_bytes)?;
    let queued =
        AdmittedWriteBatch::reservation_bytes(limits.scope_bytes, limits.scope_operations)?;
    bound(queued, config.queued_write_bytes)
}

fn metadata_owned_bytes(metadata: &std::collections::HashMap<String, String>) -> Result<usize> {
    metadata.iter().try_fold(0usize, |bytes, (key, value)| {
        bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(LiveStateError::InvalidLimit)
    })
}
fn schema_owned_bytes(schema: &arrow_schema::Schema) -> Result<usize> {
    let mut bytes = 128usize
        .checked_add(metadata_owned_bytes(schema.metadata())?)
        .ok_or(LiveStateError::InvalidLimit)?;
    for field in schema.fields() {
        let timezone = match field.data_type() {
            DataType::Timestamp(_, Some(timezone)) => timezone.len(),
            _ => 0,
        };
        bytes = bytes
            .checked_add(512)
            .and_then(|bytes| bytes.checked_add(field.name().len()))
            .and_then(|bytes| bytes.checked_add(timezone))
            .and_then(|bytes| bytes.checked_add(metadata_owned_bytes(field.metadata()).ok()?))
            .ok_or(LiveStateError::InvalidLimit)?;
    }
    Ok(bytes)
}
fn validate_ipc_metadata<'a>(
    entries: impl Iterator<Item = arrow::ipc::KeyValue<'a>> + Clone,
    expected: &std::collections::HashMap<String, String>,
    expanded: &mut usize,
    limit: usize,
) -> Result<()> {
    let mut count = 0;
    for (index, entry) in entries.clone().enumerate() {
        let key = entry
            .key()
            .ok_or_else(|| invalid("missing IPC metadata key"))?;
        let value = entry
            .value()
            .ok_or_else(|| invalid("missing IPC metadata value"))?;
        *expanded = expanded
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(LiveStateError::InvalidLimit)?;
        bound(*expanded, limit)?;
        if entries
            .clone()
            .take(index)
            .any(|previous| previous.key() == Some(key))
        {
            return Err(invalid("duplicate IPC metadata key"));
        }
        if expected.get(key).map(String::as_str) != Some(value) {
            return Err(invalid("unexpected IPC metadata"));
        }
        count += 1;
    }
    if count != expected.len() {
        return Err(invalid("missing IPC metadata entries"));
    }
    Ok(())
}

fn ipc_field_matches(field: arrow::ipc::Field<'_>, expected: &arrow_schema::Field) -> bool {
    use arrow::ipc::{DateUnit, Precision, TimeUnit, Type};
    if field.name() != Some(expected.name().as_str())
        || field.nullable() != expected.is_nullable()
        || field.dictionary().is_some()
        || field
            .children()
            .is_some_and(|children| !children.is_empty())
    {
        return false;
    }
    let int = match expected.data_type() {
        DataType::Int8 => Some((8, true)),
        DataType::Int16 => Some((16, true)),
        DataType::Int32 => Some((32, true)),
        DataType::Int64 => Some((64, true)),
        DataType::UInt8 => Some((8, false)),
        DataType::UInt16 => Some((16, false)),
        DataType::UInt32 => Some((32, false)),
        DataType::UInt64 => Some((64, false)),
        _ => None,
    };
    if let Some((width, signed)) = int {
        return field
            .type_as_int()
            .is_some_and(|value| value.bitWidth() == width && value.is_signed() == signed);
    }
    match expected.data_type() {
        DataType::Boolean => field.type_type() == Type::Bool,
        DataType::Utf8 => field.type_type() == Type::Utf8,
        DataType::Binary => field.type_type() == Type::Binary,
        DataType::Float32 => field
            .type_as_floating_point()
            .is_some_and(|value| value.precision() == Precision::SINGLE),
        DataType::Float64 => field
            .type_as_floating_point()
            .is_some_and(|value| value.precision() == Precision::DOUBLE),
        DataType::Date32 => field
            .type_as_date()
            .is_some_and(|value| value.unit() == DateUnit::DAY),
        DataType::Date64 => field
            .type_as_date()
            .is_some_and(|value| value.unit() == DateUnit::MILLISECOND),
        DataType::Timestamp(unit, timezone) => field.type_as_timestamp().is_some_and(|value| {
            let expected_unit = match unit {
                arrow_schema::TimeUnit::Second => TimeUnit::SECOND,
                arrow_schema::TimeUnit::Millisecond => TimeUnit::MILLISECOND,
                arrow_schema::TimeUnit::Microsecond => TimeUnit::MICROSECOND,
                arrow_schema::TimeUnit::Nanosecond => TimeUnit::NANOSECOND,
            };
            value.unit() == expected_unit && value.timezone() == timezone.as_deref()
        }),
        DataType::Decimal128(precision, scale) => field.type_as_decimal().is_some_and(|value| {
            value.bitWidth() == 128
                && value.precision() == i32::from(*precision)
                && value.scale() == i32::from(*scale)
        }),
        _ => false,
    }
}

/// Validate every message before StreamReader is allowed to allocate metadata or
/// body buffers. Typed rows use a schema, one uncompressed batch, and EOS only.
fn preflight_ipc(
    mut bytes: &[u8],
    descriptor: &TableDescriptor,
    limits: TableLimits,
) -> Result<()> {
    use arrow::ipc::{MessageHeader, MetadataVersion, root_as_message};
    bound(bytes.len(), limits.decoded_bytes)?;
    let mut messages = 0;
    loop {
        let mut length = bytes
            .get(..4)
            .ok_or_else(|| invalid("truncated IPC message length"))?;
        bytes = &bytes[4..];
        if length == [255; 4] {
            length = bytes
                .get(..4)
                .ok_or_else(|| invalid("truncated IPC continuation"))?;
            bytes = &bytes[4..];
        }
        let length = i32::from_le_bytes(length.try_into().unwrap());
        if length == 0 {
            if messages != 2 || !bytes.is_empty() {
                return Err(invalid("IPC requires one batch and a final EOS"));
            }
            return Ok(());
        }
        let length =
            usize::try_from(length).map_err(|_| invalid("negative IPC metadata length"))?;
        bound(length, limits.decoded_bytes)?;
        let metadata = bytes
            .get(..length)
            .ok_or_else(|| invalid("truncated IPC metadata"))?;
        bytes = &bytes[length..];
        let message = root_as_message(metadata).map_err(|e| invalid(e.to_string()))?;
        if message
            .custom_metadata()
            .is_some_and(|metadata| !metadata.is_empty())
        {
            return Err(invalid("unexpected IPC message metadata"));
        }
        if message.version() != MetadataVersion::V5 {
            return Err(invalid("unsupported IPC metadata version"));
        }
        let body = usize::try_from(message.bodyLength())
            .map_err(|_| invalid("negative IPC body length"))?;
        bound(body, limits.decoded_bytes)?;
        let body_bytes = bytes
            .get(..body)
            .ok_or_else(|| invalid("truncated IPC body"))?;
        bytes = &bytes[body..];
        match (messages, message.header_type()) {
            (0, MessageHeader::Schema) => {
                if body != 0 {
                    return Err(invalid("IPC schema body must be empty"));
                }
                let schema = message
                    .header_as_schema()
                    .ok_or_else(|| invalid("missing IPC schema"))?;
                let mut expanded = 128;
                validate_ipc_metadata(
                    schema.custom_metadata().into_iter().flatten(),
                    descriptor.schema.metadata(),
                    &mut expanded,
                    limits.decoded_bytes,
                )?;
                let fields = schema
                    .fields()
                    .ok_or_else(|| invalid("missing IPC fields"))?;
                if fields.len() != descriptor.schema.fields().len() {
                    return Err(invalid("IPC field count mismatch"));
                }
                if schema.endianness() != arrow::ipc::Endianness::Little {
                    return Err(invalid("unsupported IPC endianness"));
                }
                for (field, expected) in fields.iter().zip(descriptor.schema.fields()) {
                    let timezone = field
                        .type_as_timestamp()
                        .and_then(|timestamp| timestamp.timezone())
                        .map_or(0, str::len);
                    expanded = expanded
                        .checked_add(512)
                        .and_then(|bytes| bytes.checked_add(field.name().map_or(0, str::len)))
                        .and_then(|bytes| bytes.checked_add(timezone))
                        .ok_or(LiveStateError::InvalidLimit)?;
                    bound(expanded, limits.decoded_bytes)?;
                    validate_ipc_metadata(
                        field.custom_metadata().into_iter().flatten(),
                        expected.metadata(),
                        &mut expanded,
                        limits.decoded_bytes,
                    )?;
                    if !ipc_field_matches(field, expected) {
                        return Err(invalid("IPC field type mismatch"));
                    }
                }
            }
            (1, MessageHeader::RecordBatch) => {
                let batch = message
                    .header_as_record_batch()
                    .ok_or_else(|| invalid("missing IPC batch"))?;
                if batch.length() != 1 || batch.compression().is_some() {
                    return Err(invalid("IPC row count/compression is unsupported"));
                }
                let nodes = batch.nodes().ok_or_else(|| invalid("missing IPC nodes"))?;
                if nodes.len() != descriptor.schema.fields().len()
                    || nodes
                        .iter()
                        .any(|node| node.length() != 1 || !(0..=1).contains(&node.null_count()))
                {
                    return Err(invalid("invalid IPC node lengths"));
                }
                let buffers = batch
                    .buffers()
                    .ok_or_else(|| invalid("missing IPC buffers"))?;
                let expected = descriptor
                    .schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if matches!(field.data_type(), DataType::Utf8 | DataType::Binary) {
                            3
                        } else {
                            2
                        }
                    })
                    .sum::<usize>();
                if buffers.len() != expected {
                    return Err(invalid("invalid IPC buffer count"));
                }
                let mut end = 0;
                for buffer in buffers {
                    let offset = usize::try_from(buffer.offset())
                        .map_err(|_| invalid("negative IPC buffer offset"))?;
                    let length = usize::try_from(buffer.length())
                        .map_err(|_| invalid("negative IPC buffer length"))?;
                    let next = offset
                        .checked_add(length)
                        .ok_or_else(|| invalid("IPC buffer length overflow"))?;
                    if offset < end || next > body_bytes.len() {
                        return Err(invalid("IPC buffer exceeds body"));
                    }
                    end = next;
                }
            }
            _ => return Err(invalid("unsupported or extra IPC message")),
        }
        messages += 1;
    }
}

fn validate_row(
    row: &RecordBatch,
    descriptor: &TableDescriptor,
    limits: TableLimits,
) -> Result<()> {
    if row.schema() != descriptor.schema || row.num_rows() != 1 {
        return Err(invalid(
            "state row must contain exactly one row of the declared schema",
        ));
    }
    bound(row.get_array_memory_size(), limits.decoded_bytes)?;
    for (field, column) in row.schema().fields().iter().zip(row.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(invalid(format!("null in non-null field {}", field.name())));
        }
    }
    Ok(())
}
struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("state row encoded byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode(row: &RecordBatch, descriptor: &TableDescriptor, limit: usize) -> Result<Vec<u8>> {
    let mut sink = LimitedBuffer {
        bytes: Vec::new(),
        limit,
    };
    sink.write_all(MAGIC).map_err(|e| invalid(e.to_string()))?;
    sink.write_all(&(descriptor.schema_identity.len() as u32).to_be_bytes())
        .map_err(|e| invalid(e.to_string()))?;
    sink.write_all(&descriptor.schema_identity)
        .map_err(|e| invalid(e.to_string()))?;
    let mut writer =
        StreamWriter::try_new(&mut sink, &descriptor.schema).map_err(|e| invalid(e.to_string()))?;
    writer.write(row).map_err(|e| invalid(e.to_string()))?;
    writer.finish().map_err(|e| invalid(e.to_string()))?;
    drop(writer);
    Ok(sink.bytes)
}
fn decode(
    bytes: &[u8],
    descriptor: &TableDescriptor,
    limits: TableLimits,
    permit: Arc<ResourcePermit>,
) -> Result<TableRow> {
    bound(bytes.len(), limits.row_bytes)?;
    if bytes.len() < 12 || &bytes[..8] != MAGIC {
        return Err(invalid("unsupported state row encoding"));
    }
    let identity_bytes = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let end = 12usize
        .checked_add(identity_bytes)
        .ok_or_else(|| invalid("schema identity overflow"))?;
    if bytes.get(12..end) != Some(descriptor.schema_identity.as_slice()) {
        return Err(invalid("state row schema identity mismatch"));
    }
    preflight_ipc(&bytes[end..], descriptor, limits)?;
    let mut reader = StreamReader::try_new(Cursor::new(&bytes[end..]), None)
        .map_err(|e| invalid(e.to_string()))?;
    if reader.schema() != descriptor.schema {
        return Err(invalid("state row schema mismatch"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("missing state row"))?
        .map_err(|e| invalid(e.to_string()))?;
    validate_row(&batch, descriptor, limits)?;
    if reader.next().is_some() {
        return Err(invalid("multiple state rows in one record"));
    }
    Ok(TableRow {
        batch,
        _permit: permit,
    })
}

#[cfg(test)]
mod tests;
