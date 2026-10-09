//! Checkpoint progress compatibility: unsigned microseconds retain their legacy
//! meaning; an additive field carries exact negative Arrow nanoseconds.
use anyhow::{Result, anyhow, bail};
use arroyo_types::event_time::{from_signed_nanos, to_signed_nanos};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn encode_progress(time: Option<SystemTime>) -> Result<(Option<u64>, Option<i64>)> {
    let Some(time) = time else {
        return Ok((None, None));
    };
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => Ok((Some(u64::try_from(duration.as_micros())?), None)),
        Err(_) => Ok((
            None,
            Some(to_signed_nanos(time).ok_or_else(|| {
                anyhow!("negative checkpoint progress exceeds signed nanosecond range")
            })?),
        )),
    }
}

pub fn decode_progress(
    micros: Option<u64>,
    negative_nanos: Option<i64>,
) -> Result<Option<SystemTime>> {
    match (micros, negative_nanos) {
        (Some(_), Some(_)) => {
            bail!("checkpoint progress has conflicting unsigned and negative timestamps")
        }
        (None, Some(nanos)) => {
            if nanos >= 0 {
                bail!("negative checkpoint progress must precede the Unix epoch");
            }
            Ok(Some(from_signed_nanos(nanos).ok_or_else(|| {
                anyhow!("negative checkpoint progress exceeds SystemTime range")
            })?))
        }
        (Some(micros), None) => Ok(Some(
            UNIX_EPOCH
                .checked_add(Duration::from_micros(micros))
                .ok_or_else(|| anyhow!("checkpoint progress exceeds SystemTime range"))?,
        )),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::rpc::{OperatorMetadata, SubtaskCheckpointMetadata};
    use prost::Message;

    #[test]
    fn legacy_positive_checkpoint_and_exact_negative_progress() {
        // Historical protobuf field 4, unsigned micros = 2_000_000.
        let legacy = [0x20, 0x80, 0x89, 0x7a];
        let restored = SubtaskCheckpointMetadata::decode(legacy.as_slice()).unwrap();
        assert_eq!(
            decode_progress(restored.watermark, restored.watermark_negative_nanos).unwrap(),
            Some(UNIX_EPOCH + Duration::from_secs(2))
        );
        assert_eq!(restored.encode_to_vec(), legacy);
        for nanos in [-2_000_000_000, -1, i64::MIN] {
            let time = from_signed_nanos(nanos).unwrap();
            let (watermark, watermark_negative_nanos) = encode_progress(Some(time)).unwrap();
            let encoded = SubtaskCheckpointMetadata {
                watermark,
                watermark_negative_nanos,
                ..Default::default()
            }
            .encode_to_vec();
            let restored = SubtaskCheckpointMetadata::decode(encoded.as_slice()).unwrap();
            assert_eq!(
                decode_progress(restored.watermark, restored.watermark_negative_nanos).unwrap(),
                Some(time)
            );
            let metadata = OperatorMetadata {
                min_watermark: watermark,
                min_watermark_negative_nanos: watermark_negative_nanos,
                ..Default::default()
            };
            let restored = OperatorMetadata::decode(metadata.encode_to_vec().as_slice()).unwrap();
            assert_eq!(
                decode_progress(
                    restored.min_watermark,
                    restored.min_watermark_negative_nanos
                )
                .unwrap(),
                Some(time)
            );
        }
        assert!(decode_progress(Some(0), Some(-1)).is_err());
        assert!(decode_progress(None, Some(0)).is_err());
    }
}
