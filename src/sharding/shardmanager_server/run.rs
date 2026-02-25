use crate::discovery::RemoteShardDirectory;
use crate::TcpAddress;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn run<RD: RemoteShardDirectory<TcpAddress, String>>(
    listen_addr: String,
    rng: crate::Rng,
    remote: Arc<RD>,
    #[cfg(feature = "tls")] tls_config: Option<crate::tls::TlsConfig>,
) {
    let addr: std::net::SocketAddr = listen_addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid shard-manager listen address '{listen_addr}': {e}"));

    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("shard-manager: failed to bind {addr}: {e}"));

    tracing::info!(%addr, "shard-manager server starting");

    #[cfg(feature = "tls")]
    let tls_acceptor = tls_config.as_ref().map(|c| {
        crate::tls::ReloadableTlsAcceptor::new(c)
            .unwrap_or_else(|e| panic!("shard-manager TLS config error: {e}"))
    });

    super::serve::serve(
        listener,
        rng,
        remote,
        #[cfg(feature = "tls")]
        tls_acceptor,
        #[cfg(feature = "tls")]
        tls_config,
    )
    .await;
}
