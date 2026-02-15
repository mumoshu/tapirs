use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;

/// Network address for the Tokio bitcode TCP transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TcpAddress(pub SocketAddr);

impl fmt::Display for TcpAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SocketAddr> for TcpAddress {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

impl std::str::FromStr for TcpAddress {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<SocketAddr>().map(TcpAddress)
    }
}

impl TcpAddress {
    pub fn socket_addr(&self) -> SocketAddr {
        self.0
    }
}
