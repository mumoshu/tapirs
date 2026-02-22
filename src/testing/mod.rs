pub mod cluster;
pub mod discovery;

pub fn test_rng(seed: u64) -> crate::Rng {
    crate::Rng::from_seed(seed)
}

pub fn init_tracing() {
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    );
}
