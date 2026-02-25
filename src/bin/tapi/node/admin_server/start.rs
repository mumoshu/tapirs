use super::Node;
use std::sync::Arc;
use tokio::net::TcpListener;

pub(crate) async fn start(
    addr: std::net::SocketAddr,
    node: Arc<Node>,
    #[cfg(feature = "tls")] tls_acceptor: Option<tapirs::tls::ReloadableTlsAcceptor>,
) {
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("admin: failed to bind {addr}: {e}"));
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let node = Arc::clone(&node);

                    #[cfg(feature = "tls")]
                    let tls_acceptor = tls_acceptor.clone();

                    tokio::spawn(async move {
                        #[cfg(feature = "tls")]
                        if let Some(ref acceptor) = tls_acceptor {
                            let tls_acceptor = acceptor.acceptor();
                            match tls_acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let (reader, writer) = tokio::io::split(tls_stream);
                                    super::handle_admin_connection(reader, writer, &node).await;
                                }
                                Err(e) => {
                                    tracing::warn!("admin TLS accept error: {e}");
                                }
                            }
                            return;
                        }

                        let (reader, writer) = stream.into_split();
                        super::handle_admin_connection(reader, writer, &node).await;
                    });
                }
                Err(e) => {
                    tracing::warn!("admin accept error: {e}");
                }
            }
        }
    });
}
