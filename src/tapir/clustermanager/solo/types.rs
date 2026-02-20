/// Errors from solo cluster operations.
#[derive(Debug)]
pub enum CloneError {
    /// Source shard returned no data (possibly not bootstrapped).
    EmptySource,
    /// Admin API communication error.
    AdminError(String),
}

impl std::fmt::Display for CloneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloneError::EmptySource => write!(f, "source shard returned no data"),
            CloneError::AdminError(msg) => write!(f, "admin error: {msg}"),
        }
    }
}

/// Operations that work via direct node access without depending on
/// discovery store or ShardManager.
///
/// `clone_shard()` takes pre-constructed ShardClients (for tests with
/// ChannelTransport). `clone_shard_direct()` takes node admin addresses,
/// queries nodes to discover membership, creates destination replicas
/// via admin API, then delegates to `clone_shard()`.
pub struct SoloClusterManager {
    pub(crate) rng: crate::Rng,
    pub(crate) on_progress: Option<Box<dyn Fn(&str) + Send + Sync>>,
}

impl SoloClusterManager {
    pub fn new(rng: crate::Rng) -> Self {
        Self {
            rng,
            on_progress: None,
        }
    }

    pub fn set_progress_callback(&mut self, cb: impl Fn(&str) + Send + Sync + 'static) {
        self.on_progress = Some(Box::new(cb));
    }

    pub(crate) fn report_progress(&self, phase: &str) {
        if let Some(ref cb) = self.on_progress {
            cb(phase);
        }
    }
}
