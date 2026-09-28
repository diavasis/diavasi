use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio_util::codec::{Decoder, Encoder};

use crate::bench_protocol::Envelope;

/// Length-prefixed protobuf frames for TCP and QUIC.
#[derive(Debug, Default, Clone)]
pub struct EnvelopeCodec;

impl Encoder<Envelope> for EnvelopeCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Envelope, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.encoded_len();
        if len > u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "envelope too large",
            ));
        }
        dst.reserve(4 + len);
        dst.put_u32(len as u32);
        item.encode(dst)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(())
    }
}

impl Decoder for EnvelopeCodec {
    type Item = Envelope;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;
        if src.len() < 4 + len {
            return Ok(None);
        }
        src.advance(4);
        let payload = src.split_to(len);
        let env = Envelope::decode(payload.as_ref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(env))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench_protocol::Envelope;

    #[test]
    fn codec_round_trip() {
        let mut codec = EnvelopeCodec;
        let mut buf = BytesMut::new();
        let env = Envelope::join_group("g", "c");
        codec.encode(env.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.version, env.version);
    }
}
