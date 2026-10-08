//! Versioned binary encoding. Namespace boundaries and full keys are preserved;
//! escaped key bytes retain their lexicographic order.
use super::{LiveStateError, Ownership, Result, StateKey, StateNamespace};
const VERSION: u8 = 1;
fn invalid() -> LiveStateError {
    LiveStateError::InvalidEncoding("malformed or unsupported live state encoding".into())
}
fn blob(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| invalid())?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}
/// Measures the complete namespace without allocating encoded copies.
pub fn encoded_namespace_size(ns: &StateNamespace) -> Result<usize> {
    let blob_size = |bytes: &[u8]| -> Result<usize> {
        u32::try_from(bytes.len()).map_err(|_| invalid())?;
        bytes.len().checked_add(4).ok_or_else(invalid)
    };
    let ownership: usize = match &ns.ownership {
        Ownership::PartitionLocal {
            subtask,
            parallelism,
        } => {
            if *parallelism == 0 || subtask >= parallelism {
                return Err(invalid());
            }
            8
        }
        Ownership::Routed {
            range_start,
            range_end,
        } => {
            if range_start > range_end {
                return Err(invalid());
            }
            16
        }
        Ownership::Replicated { id } => blob_size(id)?,
        Ownership::Connector {
            connector,
            partition,
        } => blob_size(connector)?
            .checked_add(blob_size(partition)?)
            .ok_or_else(invalid)?,
    };
    ownership
        .checked_add(2)
        .and_then(|size| size.checked_add(blob_size(&ns.table).ok()?))
        .ok_or_else(invalid)
}
/// Validates routing and measures escaped full keys before budget admission.
pub fn encoded_key_size(key: &StateKey) -> Result<usize> {
    let suffix = match (&key.namespace.ownership, key.routing_hash) {
        (
            Ownership::Routed {
                range_start,
                range_end,
            },
            Some(hash),
        ) if hash >= *range_start && hash <= *range_end => 8,
        (Ownership::Routed { .. }, _) => return Err(invalid()),
        (_, None) => 0,
        (_, Some(_)) => return Err(invalid()),
    };
    encoded_namespace_size(&key.namespace)?
        .checked_add(key.key.len())
        .and_then(|size| size.checked_add(key.key.iter().filter(|byte| **byte == 0).count()))
        .and_then(|size| size.checked_add(2 + suffix))
        .ok_or_else(invalid)
}
pub fn encoded_value_size(value: &[u8]) -> Result<usize> {
    value.len().checked_add(1).ok_or_else(invalid)
}
pub fn encode_namespace(ns: &StateNamespace) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(encoded_namespace_size(ns)?);
    out.push(VERSION);
    match &ns.ownership {
        Ownership::PartitionLocal {
            subtask,
            parallelism,
        } => {
            if *parallelism == 0 || subtask >= parallelism {
                return Err(invalid());
            }
            out.push(0);
            out.extend_from_slice(&subtask.to_be_bytes());
            out.extend_from_slice(&parallelism.to_be_bytes());
        }
        Ownership::Routed {
            range_start,
            range_end,
        } => {
            if range_start > range_end {
                return Err(invalid());
            }
            out.push(1);
            out.extend_from_slice(&range_start.to_be_bytes());
            out.extend_from_slice(&range_end.to_be_bytes());
        }
        Ownership::Replicated { id } => {
            out.push(2);
            blob(&mut out, id)?;
        }
        Ownership::Connector {
            connector,
            partition,
        } => {
            out.push(3);
            blob(&mut out, connector)?;
            blob(&mut out, partition)?;
        }
    }
    blob(&mut out, &ns.table)?;
    Ok(out)
}
pub fn encode_key(key: &StateKey) -> Result<Vec<u8>> {
    let capacity = encoded_key_size(key)?;
    let mut out = encode_namespace(&key.namespace)?;
    out.reserve(capacity.saturating_sub(out.len()));
    match (&key.namespace.ownership, key.routing_hash) {
        (
            Ownership::Routed {
                range_start,
                range_end,
            },
            Some(hash),
        ) if hash >= *range_start && hash <= *range_end => (),
        (Ownership::Routed { .. }, _) => return Err(invalid()),
        (_, None) => (),
        (_, Some(_)) => return Err(invalid()),
    }
    for byte in &key.key {
        if *byte == 0 {
            out.extend_from_slice(&[0, 255]);
        } else {
            out.push(*byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
    if let Some(hash) = key.routing_hash {
        out.extend_from_slice(&hash.to_be_bytes());
    }
    Ok(out)
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(invalid());
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn blob(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
}
pub fn decode_key(bytes: &[u8]) -> Result<StateKey> {
    let mut r = Reader(bytes);
    if r.byte()? != VERSION {
        return Err(invalid());
    }
    let ownership = match r.byte()? {
        0 => Ownership::PartitionLocal {
            subtask: r.u32()?,
            parallelism: r.u32()?,
        },
        1 => Ownership::Routed {
            range_start: r.u64()?,
            range_end: r.u64()?,
        },
        2 => Ownership::Replicated { id: r.blob()? },
        3 => Ownership::Connector {
            connector: r.blob()?,
            partition: r.blob()?,
        },
        _ => return Err(invalid()),
    };
    let table = r.blob()?;
    let mut key = vec![];
    loop {
        let b = r.byte()?;
        if b != 0 {
            key.push(b);
        } else {
            match r.byte()? {
                0 => break,
                255 => key.push(0),
                _ => return Err(invalid()),
            }
        }
    }
    let routing_hash = if matches!(ownership, Ownership::Routed { .. }) {
        Some(r.u64()?)
    } else {
        None
    };
    if !r.0.is_empty() {
        return Err(invalid());
    }
    let result = StateKey {
        namespace: StateNamespace { ownership, table },
        key,
        routing_hash,
    };
    encode_key(&result)?;
    Ok(result)
}
pub fn encode_value(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(VERSION);
    out.extend_from_slice(value);
    out
}
pub fn decode_value(value: &[u8]) -> Result<Vec<u8>> {
    if value.first() != Some(&VERSION) {
        return Err(invalid());
    }
    Ok(value[1..].to_vec())
}
