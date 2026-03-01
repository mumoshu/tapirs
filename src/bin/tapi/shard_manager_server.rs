use crate::discovery_backend::DiscoveryBackend;
use rand::{thread_rng, Rng as _};
use std::sync::Arc;
use tapirs::{TcpAddress, TcpTransport, TapirReplica};

pub async fn run(
    listen_addr: String,
    discovery_tapir_endpoint: Option<String>,
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) {
    let backend = if let Some(endpoint) = discovery_tapir_endpoint {
        // Create a separate transport for the discovery cluster.
        let ephemeral_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let a = l.local_addr().unwrap();
            drop(l);
            TcpAddress(a)
        };
        let disc_dir = Arc::new(tapirs::discovery::InMemoryShardDirectory::new());
        #[cfg(feature = "tls")]
        let disc_transport: TcpTransport<TapirReplica<String, String>> = if let Some(ref tls) = tls_config {
            TcpTransport::with_tls(ephemeral_addr, disc_dir, tls)
                .unwrap_or_else(|e| panic!("discovery transport TLS error: {e}"))
        } else {
            TcpTransport::with_directory(ephemeral_addr, disc_dir)
        };
        #[cfg(not(feature = "tls"))]
        let disc_transport: TcpTransport<TapirReplica<String, String>> =
            TcpTransport::with_directory(ephemeral_addr, disc_dir);

        // Consistency: The shard-manager is the authority for shard membership
        // and route changes. strong_* methods use linearizable reads (RO
        // transactions with quorum validation) to ensure they always read the
        // latest committed state. This is critical for correctness:
        // split/merge/compact decisions must be based on the current shard
        // layout, not stale data.
        let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
        let dir = tapirs::discovery::tapir::parse_tapir_endpoint::<TcpAddress, _>(
            &endpoint,
            disc_transport,
            rng,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to create TAPIR discovery backend: {e}");
            std::process::exit(1);
        });
        DiscoveryBackend::Tapir(dir)
    } else {
        eprintln!("error: --discovery-tapir-endpoint is required");
        std::process::exit(1);
    };

    let rng = tapirs::Rng::from_seed(thread_rng().r#gen());
    tapirs::sharding::shardmanager_server::run(
        listen_addr,
        rng,
        Arc::new(backend),
        #[cfg(feature = "tls")]
        tls_config,
    )
    .await;
}
