use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

/// Maximum frame size: 16 MB.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

#[derive(Debug)]
pub(crate) enum CodecError {
    FrameTooLarge(u32),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::FrameTooLarge(size) => write!(f, "frame too large: {size} bytes"),
        }
    }
}

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
    pub fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, bitcode::Error> {
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
    pub fn try_read_frame(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
        if self.len < 4 {
            return Ok(None);
        }
        let frame_len = u32::from_le_bytes(self.buf[..4].try_into().unwrap());
        if frame_len > MAX_FRAME_SIZE {
            return Err(CodecError::FrameTooLarge(frame_len));
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
