//! Signed event-time serialization, separate from unsigned wall-clock helpers.
use crate::Watermark;
use bincode::de::Decoder;
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode, impl_borrow_decode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Converts Arrow's entire signed nanosecond domain without losing precision.
pub fn from_signed_nanos(nanos: i64) -> Option<SystemTime> {
    let duration = Duration::from_nanos(nanos.unsigned_abs());
    if nanos < 0 {
        UNIX_EPOCH.checked_sub(duration)
    } else {
        UNIX_EPOCH.checked_add(duration)
    }
}

pub fn to_signed_nanos(time: SystemTime) -> Option<i64> {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::try_from(duration.as_nanos()).ok()?,
        Err(error) => -i128::try_from(error.duration().as_nanos()).ok()?,
    };
    i64::try_from(nanos).ok()
}

// Keep the original enum discriminants and positive SystemTime payload intact.
// Older peers reject variant 2 rather than interpreting negative time as positive.
#[derive(Encode, Decode)]
enum WireWatermark {
    EventTime(SystemTime),
    Idle,
    PreEpoch(Duration),
}

impl Encode for Watermark {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let wire = match self {
            Self::EventTime(time) => match time.duration_since(UNIX_EPOCH) {
                Ok(_) => WireWatermark::EventTime(*time),
                Err(error) => WireWatermark::PreEpoch(error.duration()),
            },
            Self::Idle => WireWatermark::Idle,
        };
        wire.encode(encoder)
    }
}

impl<Context> Decode<Context> for Watermark {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        match WireWatermark::decode(decoder)? {
            WireWatermark::EventTime(time) => Ok(Self::EventTime(time)),
            WireWatermark::Idle => Ok(Self::Idle),
            WireWatermark::PreEpoch(duration) => UNIX_EPOCH
                .checked_sub(duration)
                .map(Self::EventTime)
                .ok_or(DecodeError::Other(
                    "pre-epoch watermark exceeds SystemTime range",
                )),
        }
    }
}

impl_borrow_decode!(Watermark);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SignalMessage;

    #[test]
    fn legacy_watermark_bytes_and_negative_signals_round_trip() {
        let config = bincode::config::standard();
        // Historical enum 0 + SystemTime(Duration { secs: 2, nanos: 0 }).
        let legacy = [0, 2, 0];
        let positive = Watermark::EventTime(UNIX_EPOCH + Duration::from_secs(2));
        assert_eq!(bincode::encode_to_vec(positive, config).unwrap(), legacy);
        assert_eq!(
            bincode::decode_from_slice::<Watermark, _>(&legacy, config)
                .unwrap()
                .0,
            positive
        );
        assert_eq!(
            bincode::encode_to_vec(Watermark::Idle, config).unwrap(),
            [1]
        );
        for nanos in [-2_000_000_000, -1, i64::MIN] {
            let message =
                SignalMessage::Watermark(Watermark::EventTime(from_signed_nanos(nanos).unwrap()));
            let bytes = bincode::encode_to_vec(&message, config).unwrap();
            let (restored, used) =
                bincode::decode_from_slice::<SignalMessage, _>(&bytes, config).unwrap();
            assert_eq!(used, bytes.len());
            assert_eq!(restored, message);
        }
    }

    #[test]
    fn signed_arrow_domain_and_positive_end_of_data_sentinel() {
        for nanos in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert_eq!(
                to_signed_nanos(from_signed_nanos(nanos).unwrap()),
                Some(nanos)
            );
        }
        let end = crate::from_nanos(u64::MAX as u128);
        assert_eq!(to_signed_nanos(end), None);
        let watermark = Watermark::EventTime(end);
        let bytes = bincode::encode_to_vec(watermark, bincode::config::standard()).unwrap();
        assert_eq!(
            bincode::decode_from_slice::<Watermark, _>(&bytes, bincode::config::standard())
                .unwrap()
                .0,
            watermark
        );
    }
}
