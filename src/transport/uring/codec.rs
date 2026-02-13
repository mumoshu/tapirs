use super::error::UringError;
use serde::{de::DeserializeOwned, Serialize};

/// Maximum frame size: 16 MB.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Length-prefixed frame codec (4-byte LE u32 + bitcode payload).
pub(crate) struct FrameCodec;

impl FrameCodec {
    /// Encode a message into a length-prefixed frame.
    pub fn encode<T: Serialize>(msg: &T) -> Result<Vec<u8>, bitcode::Error> {
        let payload = bitcode::serialize(msg)?;
        let len = payload.len() as u32;
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Decode a bitcode payload (without length prefix).
    pub fn decode<T: DeserializeOwned>(
        payload: &[u8],
    ) -> Result<T, bitcode::Error> {
        bitcode::deserialize(payload)
    }
}

/// Per-connection read buffer for reassembling partial reads.
pub(crate) struct FrameReader {
    buf: Vec<u8>,
    /// Number of valid bytes in `buf`.
    len: usize,
}

impl FrameReader {
    pub fn new() -> Self {
        Self {
            buf: vec![0u8; 8192],
            len: 0,
        }
    }

    /// Returns the writable portion of the buffer for recv.
    pub fn recv_buf(&mut self) -> &mut [u8] {
        if self.len == self.buf.len() {
            self.buf.resize(self.buf.len() * 2, 0);
        }
        &mut self.buf[self.len..]
    }

    /// Advance the valid-data cursor after a recv.
    pub fn advance(&mut self, n: usize) {
        self.len += n;
    }

    /// Try to extract one complete frame from the buffer.
    /// Returns the payload bytes (without length prefix).
    pub fn try_read_frame(
        &mut self,
    ) -> Result<Option<Vec<u8>>, UringError> {
        if self.len < 4 {
            return Ok(None);
        }
        let frame_len =
            u32::from_le_bytes(self.buf[..4].try_into().unwrap());
        if frame_len > MAX_FRAME_SIZE {
            return Err(UringError::Codec(format!(
                "frame too large: {frame_len} bytes"
            )));
        }
        let total = 4 + frame_len as usize;
        if self.len < total {
            return Ok(None);
        }
        let payload = self.buf[4..total].to_vec();
        // Shift remaining data to front.
        self.buf.copy_within(total..self.len, 0);
        self.len -= total;
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let msg: Vec<u32> = vec![1, 2, 3, 42];
        let frame = FrameCodec::encode(&msg).unwrap();
        // First 4 bytes are length prefix.
        let len =
            u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4);
        let decoded: Vec<u32> =
            FrameCodec::decode(&frame[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn frame_reader_partial_reads() {
        let msg: String = "hello".to_string();
        let frame = FrameCodec::encode(&msg).unwrap();
        let mut reader = FrameReader::new();

        // Feed one byte at a time.
        for &b in &frame[..frame.len() - 1] {
            reader.recv_buf()[0] = b;
            reader.advance(1);
            assert!(reader.try_read_frame().unwrap().is_none());
        }
        // Feed last byte.
        let last = *frame.last().unwrap();
        reader.recv_buf()[0] = last;
        reader.advance(1);
        let payload = reader.try_read_frame().unwrap().unwrap();
        let decoded: String = FrameCodec::decode(&payload).unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn frame_reader_two_frames() {
        let f1 = FrameCodec::encode(&42u64).unwrap();
        let f2 = FrameCodec::encode(&99u64).unwrap();
        let mut reader = FrameReader::new();
        let buf = reader.recv_buf();
        buf[..f1.len()].copy_from_slice(&f1);
        buf[f1.len()..f1.len() + f2.len()].copy_from_slice(&f2);
        reader.advance(f1.len() + f2.len());

        let p1 = reader.try_read_frame().unwrap().unwrap();
        assert_eq!(FrameCodec::decode::<u64>(&p1).unwrap(), 42);
        let p2 = reader.try_read_frame().unwrap().unwrap();
        assert_eq!(FrameCodec::decode::<u64>(&p2).unwrap(), 99);
        assert!(reader.try_read_frame().unwrap().is_none());
    }

    #[test]
    fn test_max_frame_size_roundtrip() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        // Create exactly MAX_FRAME_SIZE (16 MB) payload
        let mut rng = StdRng::seed_from_u64(42);
        let payload: Vec<u8> = (0..MAX_FRAME_SIZE).map(|_| rng.r#gen()).collect();

        // Encode and decode
        let frame = FrameCodec::encode(&payload).unwrap();
        assert_eq!(frame.len(), 4 + MAX_FRAME_SIZE as usize);

        let decoded: Vec<u8> = FrameCodec::decode(&frame[4..]).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_oversized_frame_rejection() {
        let mut reader = FrameReader::new();

        // Manually craft a frame with length prefix = MAX_FRAME_SIZE + 1
        let oversized_len = MAX_FRAME_SIZE + 1;
        let len_bytes = oversized_len.to_le_bytes();

        let buf = reader.recv_buf();
        buf[..4].copy_from_slice(&len_bytes);
        reader.advance(4);

        // Should return error for oversized frame
        match reader.try_read_frame() {
            Err(UringError::Codec(msg)) => {
                assert!(msg.contains("frame too large"));
                assert!(msg.contains(&oversized_len.to_string()));
            }
            _ => panic!("Expected Codec error for oversized frame"),
        }
    }

    #[test]
    fn test_buffer_growth() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        // Create 1 MB payload (FrameReader starts at 8KB)
        let mut rng = StdRng::seed_from_u64(123);
        let large_payload: Vec<u8> = (0..1024 * 1024).map(|_| rng.r#gen()).collect();

        // Encode the frame
        let frame = FrameCodec::encode(&large_payload).unwrap();

        // Feed to FrameReader (buffer should grow dynamically)
        let mut reader = FrameReader::new();
        assert_eq!(reader.buf.len(), 8192); // Initial size

        let buf = reader.recv_buf();
        buf[..frame.len()].copy_from_slice(&frame);
        reader.advance(frame.len());

        // Buffer should have grown to accommodate
        assert!(reader.buf.len() >= frame.len());

        // Frame should decode successfully
        let decoded_payload = reader.try_read_frame().unwrap().unwrap();
        let decoded: Vec<u8> = FrameCodec::decode(&decoded_payload).unwrap();
        assert_eq!(decoded, large_payload);
    }

    #[test]
    fn test_corrupted_bitcode_payload() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        // Create valid length prefix but random garbage payload
        let mut rng = StdRng::seed_from_u64(456);
        let garbage: Vec<u8> = (0..100).map(|_| rng.r#gen()).collect();
        let len = 100u32;

        let mut frame = Vec::new();
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&garbage);

        let mut reader = FrameReader::new();
        let buf = reader.recv_buf();
        buf[..frame.len()].copy_from_slice(&frame);
        reader.advance(frame.len());

        // Frame should be extracted successfully
        let payload = reader.try_read_frame().unwrap().unwrap();

        // But decoding should fail with bitcode error
        let result: Result<Vec<u8>, _> = FrameCodec::decode(&payload);
        assert!(result.is_err(), "Expected bitcode decode error for garbage payload");
    }
}
