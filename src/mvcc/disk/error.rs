use std::fmt;
use std::io;

/// Errors from the disk-backed storage engine.
#[derive(Debug)]
pub enum StorageError {
    /// Data corruption detected (CRC mismatch).
    Corruption {
        file: String,
        offset: u64,
        expected_crc: u32,
        actual_crc: u32,
    },
    /// Underlying I/O error.
    Io(io::Error),
    /// Serialization/deserialization error.
    Codec(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corruption {
                file,
                offset,
                expected_crc,
                actual_crc,
            } => write!(
                f,
                "corruption in {file} at offset {offset}: \
                 expected CRC {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Codec(msg) => write!(f, "codec error: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<io::Error> for StorageError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
