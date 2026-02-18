use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng};

/// Deterministic random number generator for IR and TAPIR components.
///
/// This is the **sole randomness abstraction** for the tapi-rs library.
/// All IR and TAPIR modules depend on `crate::Rng` only — they never import
/// the `rand` crate directly. Only this module (`src/rng.rs`) depends on
/// `rand`, keeping the external RNG dependency isolated in one place.
///
/// The two operations exposed — [`Rng::random_u64`] and [`Rng::random_index`]
/// — cover every randomness need in the protocol stack:
/// - `random_u64()`: generating `IrClientId` values and initial transaction
///   numbers.
/// - `random_index(count)`: selecting which replica to query in
///   `invoke_unlogged` and `fetch_leader_record`.
///
/// # Deterministic testing
///
/// Construct with [`Rng::from_seed`] using a fixed seed for fully reproducible
/// behavior in simulation tests (`start_paused = true`). Use [`Rng::fork`] to
/// derive independent child `Rng`s for sub-components (e.g., when a
/// `TapirClient` creates per-shard `ShardClient`s) without coupling their
/// random sequences.
///
/// # Production usage
///
/// Binaries create an `Rng` seeded from system entropy:
/// ```ignore
/// use rand::{thread_rng, Rng as _};
/// let rng = tapi_rs::Rng::from_seed(thread_rng().gen());
/// ```
pub struct Rng(StdRng);

impl Rng {
    /// Create an Rng from a fixed seed (deterministic).
    pub fn from_seed(seed: u64) -> Self {
        Self(StdRng::seed_from_u64(seed))
    }

    /// Generate a random u64. Used for IrClientId and transaction numbers.
    pub fn random_u64(&mut self) -> u64 {
        self.0.r#gen()
    }

    /// Pick a random index in `0..count`. Used for replica selection.
    pub fn random_index(&mut self, count: usize) -> usize {
        self.0.gen_range(0..count)
    }

    /// Derive an independent child Rng for a sub-component.
    /// The child's sequence is deterministic but independent from the parent's.
    pub fn fork(&mut self) -> Self {
        Self(StdRng::seed_from_u64(self.random_u64()))
    }
}
