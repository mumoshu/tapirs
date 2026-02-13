use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;

/// Network address for the io_uring transport (a TCP socket address).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
pub struct UringAddress(pub SocketAddr);

impl fmt::Display for UringAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SocketAddr> for UringAddress {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

impl UringAddress {
    pub fn socket_addr(&self) -> SocketAddr {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_serde() {
        let addr: UringAddress =
            "127.0.0.1:8080".parse::<SocketAddr>().unwrap().into();
        let bytes = bitcode::serialize(&addr).unwrap();
        let back: UringAddress = bitcode::deserialize(&bytes).unwrap();
        assert_eq!(addr, back);
    }

    #[test]
    fn display() {
        let addr = UringAddress("127.0.0.1:9090".parse().unwrap());
        assert_eq!(addr.to_string(), "127.0.0.1:9090");
    }

    #[test]
    fn copy_and_eq() {
        let a = UringAddress("10.0.0.1:80".parse().unwrap());
        let b = a; // Copy
        assert_eq!(a, b);
    }
}
