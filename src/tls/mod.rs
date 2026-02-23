mod reload;

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub use reload::spawn_reload_task;

/// Paths to PEM-encoded certificate, private key, and CA bundle files.
///
/// This is the on-disk layout cert-manager writes to Kubernetes secrets:
/// - `cert_path`: `tls.crt` (leaf certificate chain)
/// - `key_path`: `tls.key` (private key)
/// - `ca_path`: `ca.crt` (CA bundle for peer verification)
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
}

/// Server-side TLS acceptor with hot-reloadable certificates.
///
/// Wraps an `ArcSwap<ServerConfig>` so the background reload task can
/// atomically swap in new certificates without affecting existing connections.
#[derive(Clone)]
pub struct ReloadableTlsAcceptor {
    inner: Arc<ArcSwap<ServerConfig>>,
}

impl ReloadableTlsAcceptor {
    /// Build from a `TlsConfig` by loading certs from disk.
    pub fn new(config: &TlsConfig) -> Result<Self, TlsError> {
        let server_config = build_server_config(config)?;
        Ok(Self {
            inner: Arc::new(ArcSwap::from_pointee(server_config)),
        })
    }

    /// Get a `TlsAcceptor` snapshot for a single handshake.
    pub fn acceptor(&self) -> TlsAcceptor {
        let cfg = self.inner.load();
        TlsAcceptor::from(Arc::clone(&*cfg))
    }

    /// Access the inner `ArcSwap` for the reload task.
    pub(crate) fn swap_target(&self) -> &Arc<ArcSwap<ServerConfig>> {
        &self.inner
    }
}

/// Client-side TLS connector with hot-reloadable certificates.
#[derive(Clone)]
pub struct ReloadableTlsConnector {
    inner: Arc<ArcSwap<ClientConfig>>,
}

impl ReloadableTlsConnector {
    /// Build from a `TlsConfig` by loading certs from disk.
    pub fn new(config: &TlsConfig) -> Result<Self, TlsError> {
        let client_config = build_client_config(config)?;
        Ok(Self {
            inner: Arc::new(ArcSwap::from_pointee(client_config)),
        })
    }

    /// Get a `TlsConnector` snapshot for a single handshake.
    pub fn connector(&self) -> TlsConnector {
        let cfg = self.inner.load();
        TlsConnector::from(Arc::clone(&*cfg))
    }

    /// Access the inner `ArcSwap` for the reload task.
    pub(crate) fn swap_target(&self) -> &Arc<ArcSwap<ClientConfig>> {
        &self.inner
    }
}

/// Errors from TLS configuration.
#[derive(Debug)]
pub enum TlsError {
    Io(std::io::Error),
    Rustls(rustls::Error),
    NoCertificates,
    NoPrivateKey,
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io(e) => write!(f, "TLS I/O error: {e}"),
            TlsError::Rustls(e) => write!(f, "TLS config error: {e}"),
            TlsError::NoCertificates => write!(f, "no certificates found in cert file"),
            TlsError::NoPrivateKey => write!(f, "no private key found in key file"),
        }
    }
}

impl From<std::io::Error> for TlsError {
    fn from(e: std::io::Error) -> Self {
        TlsError::Io(e)
    }
}

impl From<rustls::Error> for TlsError {
    fn from(e: rustls::Error) -> Self {
        TlsError::Rustls(e)
    }
}

/// Load PEM certificates from a file.
pub fn load_certs(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates);
    }
    Ok(certs)
}

/// Load a PEM private key from a file.
pub fn load_private_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    for item in rustls_pemfile::read_all(&mut reader) {
        match item? {
            rustls_pemfile::Item::Pkcs1Key(key) => return Ok(PrivateKeyDer::Pkcs1(key)),
            rustls_pemfile::Item::Pkcs8Key(key) => return Ok(PrivateKeyDer::Pkcs8(key)),
            rustls_pemfile::Item::Sec1Key(key) => return Ok(PrivateKeyDer::Sec1(key)),
            _ => continue,
        }
    }
    Err(TlsError::NoPrivateKey)
}

/// Load a CA certificate bundle into a `RootCertStore`.
pub fn load_ca_certs(path: &std::path::Path) -> Result<RootCertStore, TlsError> {
    let certs = load_certs(path)?;
    let mut store = RootCertStore::empty();
    for cert in certs {
        store.add(cert).map_err(|e| TlsError::Rustls(e.into()))?;
    }
    Ok(store)
}

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Build a `ServerConfig` for mTLS (requires client certificates).
pub fn build_server_config(config: &TlsConfig) -> Result<ServerConfig, TlsError> {
    let certs = load_certs(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;
    let ca_store = load_ca_certs(&config.ca_path)?;

    let client_verifier =
        rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(ca_store),
            crypto_provider(),
        )
        .build()
        .map_err(|e| TlsError::Rustls(rustls::Error::General(e.to_string())))?;

    let mut cfg = ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Rustls(e))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)?;
    cfg.alpn_protocols = vec![b"tapir".to_vec()];
    Ok(cfg)
}

/// Build a `ClientConfig` for mTLS (presents client certificate).
pub fn build_client_config(config: &TlsConfig) -> Result<ClientConfig, TlsError> {
    let certs = load_certs(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;
    let ca_store = load_ca_certs(&config.ca_path)?;

    let mut cfg = ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Rustls(e))?
        .with_root_certificates(ca_store)
        .with_client_auth_cert(certs, key)?;
    cfg.alpn_protocols = vec![b"tapir".to_vec()];
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Generate a self-signed CA + leaf certificate using rcgen.
    fn generate_test_certs(
        dir: &std::path::Path,
    ) -> (PathBuf, PathBuf, PathBuf) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let mut leaf_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        leaf_params.is_ca = rcgen::IsCa::NoCa;
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        let cert_path = dir.join("tls.crt");
        let key_path = dir.join("tls.key");
        let ca_path = dir.join("ca.crt");

        std::fs::File::create(&cert_path)
            .unwrap()
            .write_all(leaf_cert.pem().as_bytes())
            .unwrap();
        std::fs::File::create(&key_path)
            .unwrap()
            .write_all(leaf_key.serialize_pem().as_bytes())
            .unwrap();
        std::fs::File::create(&ca_path)
            .unwrap()
            .write_all(ca_cert.pem().as_bytes())
            .unwrap();

        (cert_path, key_path, ca_path)
    }

    #[test]
    fn test_load_certs_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path, ca_path) = generate_test_certs(dir.path());

        let certs = load_certs(&cert_path).unwrap();
        assert!(!certs.is_empty());

        let _key = load_private_key(&key_path).unwrap();

        let store = load_ca_certs(&ca_path).unwrap();
        assert!(!store.is_empty());
    }

    #[test]
    fn test_build_server_config() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path, ca_path) = generate_test_certs(dir.path());

        let config = TlsConfig {
            cert_path,
            key_path,
            ca_path,
        };
        let _server_config = build_server_config(&config).unwrap();
    }

    #[test]
    fn test_build_client_config() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path, ca_path) = generate_test_certs(dir.path());

        let config = TlsConfig {
            cert_path,
            key_path,
            ca_path,
        };
        let _client_config = build_client_config(&config).unwrap();
    }

    #[test]
    fn test_reloadable_acceptor_and_connector() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path, ca_path) = generate_test_certs(dir.path());

        let config = TlsConfig {
            cert_path,
            key_path,
            ca_path,
        };
        let acceptor = ReloadableTlsAcceptor::new(&config).unwrap();
        let _tls_acceptor = acceptor.acceptor();

        let connector = ReloadableTlsConnector::new(&config).unwrap();
        let _tls_connector = connector.connector();
    }

    #[test]
    fn test_error_on_missing_file() {
        let result = load_certs(std::path::Path::new("/nonexistent/tls.crt"));
        assert!(result.is_err());
    }

    #[test]
    fn test_error_on_empty_cert_file() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.crt");
        std::fs::File::create(&empty).unwrap();
        let result = load_certs(&empty);
        assert!(matches!(result, Err(TlsError::NoCertificates)));
    }

    #[test]
    fn test_error_on_empty_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.key");
        std::fs::File::create(&empty).unwrap();
        let result = load_private_key(&empty);
        assert!(matches!(result, Err(TlsError::NoPrivateKey)));
    }
}
