use crate::discovery::RemoteShardDirectory;
use crate::TcpAddress;
use super::ShardManagerState;
use super::handle_connection::handle_connection;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn serve<RD: RemoteShardDirectory<TcpAddress, String>>(
    listener: TcpListener,
    rng: crate::Rng,
    remote: Arc<RD>,
    #[cfg(feature = "tls")] tls_acceptor: Option<crate::tls::ReloadableTlsAcceptor>,
    #[cfg(feature = "tls")] tls_config: Option<crate::tls::TlsConfig>,
) {
    let state = Arc::new(ShardManagerState::new(
        rng,
        remote,
        #[cfg(feature = "tls")]
        &tls_config,
    ));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);

                #[cfg(feature = "tls")]
                let tls_acceptor = tls_acceptor.clone();

                tokio::spawn(async move {
                    #[cfg(feature = "tls")]
                    if let Some(ref acceptor) = tls_acceptor {
                        let tls_acceptor = acceptor.acceptor();
                        match tls_acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                let (reader, writer) = tokio::io::split(tls_stream);
                                handle_connection(reader, writer, &state).await;
                            }
                            Err(e) => {
                                tracing::warn!("shard-manager TLS accept error: {e}");
                            }
                        }
                        return;
                    }

                    let (reader, writer) = stream.into_split();
                    handle_connection(reader, writer, &state).await;
                });
            }
            Err(e) => {
                tracing::warn!("shard-manager accept error: {e}");
            }
        }
    }
}
