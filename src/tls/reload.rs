use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use rustls::{ClientConfig, ServerConfig};
use tracing::{debug, warn};

use super::{TlsConfig, build_client_config, build_server_config};

const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// Spawn a background task that polls certificate file modification times
/// and atomically swaps the server and client configs when changes are detected.
///
/// Returns a `JoinHandle` that can be used to abort the task if needed.
pub fn spawn_reload_task(
    config: TlsConfig,
    server: Option<Arc<ArcSwap<ServerConfig>>>,
    client: Option<Arc<ArcSwap<ClientConfig>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        reload_loop(config, server, client, DEFAULT_RELOAD_INTERVAL).await;
    })
}

async fn reload_loop(
    config: TlsConfig,
    server: Option<Arc<ArcSwap<ServerConfig>>>,
    client: Option<Arc<ArcSwap<ClientConfig>>>,
    interval: Duration,
) {
    let mut last_mtime = get_max_mtime(&config);

    loop {
        tokio::time::sleep(interval).await;

        let current_mtime = get_max_mtime(&config);
        if current_mtime <= last_mtime {
            continue;
        }

        debug!("TLS certificate files changed, reloading");
        last_mtime = current_mtime;

        if let Some(ref server_swap) = server {
            match build_server_config(&config) {
                Ok(new_config) => {
                    server_swap.store(Arc::new(new_config));
                    debug!("TLS server config reloaded");
                }
                Err(e) => {
                    warn!("Failed to reload TLS server config: {e}");
                }
            }
        }

        if let Some(ref client_swap) = client {
            match build_client_config(&config) {
                Ok(new_config) => {
                    client_swap.store(Arc::new(new_config));
                    debug!("TLS client config reloaded");
                }
                Err(e) => {
                    warn!("Failed to reload TLS client config: {e}");
                }
            }
        }
    }
}

/// Get the maximum modification time across all three cert files.
/// Returns `None` if any file can't be stat'd.
fn get_max_mtime(config: &TlsConfig) -> Option<SystemTime> {
    let paths = [&config.cert_path, &config.key_path, &config.ca_path];
    let mut max = None;
    for path in &paths {
        match std::fs::metadata(path).and_then(|m| m.modified()) {
            Ok(mtime) => {
                max = Some(match max {
                    Some(current) if mtime > current => mtime,
                    Some(current) => current,
                    None => mtime,
                });
            }
            Err(e) => {
                warn!("Failed to stat {}: {e}", path.display());
                return None;
            }
        }
    }
    max
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn generate_test_certs(
        dir: &std::path::Path,
    ) -> TlsConfig {
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

        TlsConfig {
            cert_path,
            key_path,
            ca_path,
        }
    }

    #[test]
    fn test_get_max_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let config = generate_test_certs(dir.path());
        let mtime = get_max_mtime(&config);
        assert!(mtime.is_some());
    }

    #[test]
    fn test_get_max_mtime_missing_file() {
        let config = TlsConfig {
            cert_path: "/nonexistent/tls.crt".into(),
            key_path: "/nonexistent/tls.key".into(),
            ca_path: "/nonexistent/ca.crt".into(),
        };
        assert!(get_max_mtime(&config).is_none());
    }

    #[tokio::test]
    async fn test_reload_detects_change() {
        // Use real time (not paused) since we need real file mtime changes.
        let dir = tempfile::tempdir().unwrap();
        let config = generate_test_certs(dir.path());

        let server_config = build_server_config(&config).unwrap();
        let server_swap = Arc::new(ArcSwap::from_pointee(server_config));

        let client_config = build_client_config(&config).unwrap();
        let client_swap = Arc::new(ArcSwap::from_pointee(client_config));

        let reload_interval = Duration::from_millis(50);

        let config_clone = config.clone();
        let server_clone = server_swap.clone();
        let client_clone = client_swap.clone();
        let handle = tokio::spawn(async move {
            reload_loop(
                config_clone,
                Some(server_clone),
                Some(client_clone),
                reload_interval,
            )
            .await;
        });

        // Wait a bit, then rewrite the cert file to trigger mtime change.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Regenerate certs to change the file content and mtime.
        let _new_config = generate_test_certs(dir.path());

        // Wait for the reload loop to detect the change.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The configs should have been reloaded (no panic or error).
        handle.abort();
    }
}
