use std::fmt;

/// Errors from the io_uring transport layer.
#[derive(Debug)]
pub enum UringError {
    /// An io_uring or socket I/O error.
    Io(std::io::Error),
    /// Frame codec error (e.g. frame too large).
    Codec(String),
    /// Serialization/deserialization error.
    Serde(bitcode::Error),
}

impl fmt::Display for UringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Codec(msg) => write!(f, "codec error: {msg}"),
            Self::Serde(e) => write!(f, "serde error: {e}"),
        }
    }
}

impl std::error::Error for UringError {}

impl From<std::io::Error> for UringError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<bitcode::Error> for UringError {
    fn from(e: bitcode::Error) -> Self {
        Self::Serde(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_io_error() {
        let e = UringError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken",
        ));
        assert!(e.to_string().contains("broken"));
    }

    #[test]
    fn display_codec_error() {
        let e = UringError::Codec("too large".into());
        assert!(e.to_string().contains("too large"));
    }
}
