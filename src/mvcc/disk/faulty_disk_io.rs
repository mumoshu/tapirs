use super::aligned_buf::AlignedBuf;
use super::disk_io::{DiskIo, OpenFlags};
use super::error::StorageError;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::cell::RefCell;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

thread_local! {
    /// Shared fault state for testing. When enabled, all FaultyDiskIo instances
    /// created via `open()` in this thread will share the same fault state,
    /// allowing runtime configuration changes to affect all instances.
    static SHARED_FAULT_STATE: RefCell<Option<Arc<Mutex<FaultState>>>> = const { RefCell::new(None) };
}

/// Wrapper around any DiskIo implementation with configurable fault injection.
///
/// FaultyDiskIo wraps any implementation of the DiskIo trait and probabilistically
/// injects storage faults for testing crash recovery and error handling. All faults
/// are seeded and deterministic for reproducible testing.
#[derive(Clone)]
pub struct FaultyDiskIo<IO: DiskIo> {
    inner: IO,
    state: Arc<Mutex<FaultState>>,
}

/// Internal fault state shared across FaultyDiskIo instances.
///
/// Public to allow test access via `get_shared_fault_state()`.
pub struct FaultState {
    pub config: DiskFaultConfig,
    pub rng: StdRng,
    pub bytes_written: u64,
}

/// Configuration for disk fault injection.
///
/// All probability rates should be in the range [0.0, 1.0] where:
/// - 0.0 = never inject this fault
/// - 1.0 = always inject this fault
#[derive(Clone, Debug)]
pub struct DiskFaultConfig {
    /// Probability of fsync returning I/O error (0.0-1.0)
    pub fsync_fail_rate: f64,
    /// Probability of bit flips on read (0.0-1.0)
    pub read_corruption_rate: f64,
    /// Simulate ENOSPC after this many bytes written
    pub enospc_after_bytes: Option<u64>,
    /// Inject latency on all I/O operations
    pub slow_io_latency: Option<Duration>,
}

impl Default for DiskFaultConfig {
    fn default() -> Self {
        Self {
            fsync_fail_rate: 0.0,
            read_corruption_rate: 0.0,
            enospc_after_bytes: None,
            slow_io_latency: None,
        }
    }
}

/// Future that wraps any read future and injects read faults.
pub struct FaultyReadFuture<F> {
    inner: F,
    buf: *mut AlignedBuf,
    state: Arc<Mutex<FaultState>>,
    latency_applied: bool,
}

// SAFETY: Same reasoning as UringReadFuture - used in single-threaded reactor
unsafe impl<F: Send> Send for FaultyReadFuture<F> {}

impl<F: Future<Output = Result<(), StorageError>>> Future for FaultyReadFuture<F> {
    type Output = Result<(), StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We never move inner or change pinning invariants
        let this = unsafe { self.get_unchecked_mut() };

        // 1. Apply latency (once, before first poll of inner)
        if !this.latency_applied {
            if let Some(latency) = this.state.lock().unwrap().config.slow_io_latency
                && latency > Duration::ZERO
            {
                std::thread::sleep(latency);
            }
            this.latency_applied = true;
        }

        // 2. Poll inner future
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        match inner.poll(cx) {
            Poll::Ready(Ok(())) => {
                // 3. Inject read corruption
                if Self::should_corrupt(&this.state) {
                    let buf = unsafe { &mut *this.buf };
                    Self::corrupt_buffer(&this.state, buf);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> FaultyReadFuture<F> {
    fn should_corrupt(state: &Arc<Mutex<FaultState>>) -> bool {
        let mut s = state.lock().unwrap();
        let rate = s.config.read_corruption_rate;
        s.rng.gen_bool(rate)
    }

    fn corrupt_buffer(state: &Arc<Mutex<FaultState>>, buf: &mut AlignedBuf) {
        let mut s = state.lock().unwrap();
        if buf.len() == 0 {
            return;
        }
        // Flip a random byte's bits
        let idx = s.rng.gen_range(0..buf.len());
        let slice = buf.as_full_slice_mut();
        slice[idx] ^= 0xFF;
    }
}

/// Future that wraps any write future and injects write faults.
pub struct FaultyWriteFuture<F> {
    inner: F,
    buf_size: usize,
    state: Arc<Mutex<FaultState>>,
    latency_applied: bool,
    enospc_checked: bool,
}

unsafe impl<F: Send> Send for FaultyWriteFuture<F> {}

impl<F: Future<Output = Result<(), StorageError>>> Future for FaultyWriteFuture<F> {
    type Output = Result<(), StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        // 1. Check ENOSPC before attempting write
        if !this.enospc_checked {
            if Self::should_fail_enospc(&this.state, this.buf_size as u64) {
                return Poll::Ready(Err(StorageError::Io(std::io::Error::other(
                    "No space left on device (simulated)",
                ))));
            }
            this.enospc_checked = true;
        }

        // 2. Apply latency
        if !this.latency_applied {
            if let Some(latency) = this.state.lock().unwrap().config.slow_io_latency
                && latency > Duration::ZERO
            {
                std::thread::sleep(latency);
            }
            this.latency_applied = true;
        }

        // 3. Poll inner future
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        match inner.poll(cx) {
            Poll::Ready(Ok(())) => {
                // 4. Record bytes written
                this.state.lock().unwrap().bytes_written += this.buf_size as u64;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> FaultyWriteFuture<F> {
    fn should_fail_enospc(state: &Arc<Mutex<FaultState>>, write_size: u64) -> bool {
        let s = state.lock().unwrap();
        if let Some(limit) = s.config.enospc_after_bytes {
            s.bytes_written + write_size > limit
        } else {
            false
        }
    }
}

/// Future that wraps fsync and injects fsync faults.
pub struct FaultyFsyncFuture<F> {
    inner: F,
    state: Arc<Mutex<FaultState>>,
    latency_applied: bool,
}

unsafe impl<F: Send> Send for FaultyFsyncFuture<F> {}

impl<F: Future<Output = Result<(), StorageError>>> Future for FaultyFsyncFuture<F> {
    type Output = Result<(), StorageError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        // 1. Apply latency
        if !this.latency_applied {
            if let Some(latency) = this.state.lock().unwrap().config.slow_io_latency
                && latency > Duration::ZERO
            {
                std::thread::sleep(latency);
            }
            this.latency_applied = true;
        }

        // 2. Poll inner future
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        match inner.poll(cx) {
            Poll::Ready(Ok(())) => {
                // 3. Inject fsync failure
                if Self::should_fail(&this.state) {
                    Poll::Ready(Err(StorageError::Io(std::io::Error::other(
                        "injected fsync failure",
                    ))))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> FaultyFsyncFuture<F> {
    fn should_fail(state: &Arc<Mutex<FaultState>>) -> bool {
        let mut s = state.lock().unwrap();
        let rate = s.config.fsync_fail_rate;
        s.rng.gen_bool(rate)
    }
}

impl<IO: DiskIo> DiskIo for FaultyDiskIo<IO> {
    type ReadFuture = FaultyReadFuture<IO::ReadFuture>;
    type WriteFuture = FaultyWriteFuture<IO::WriteFuture>;

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, StorageError> {
        let inner = IO::open(path, flags)?;

        // Check if shared fault state is enabled (for testing)
        let shared_state = SHARED_FAULT_STATE.with(|s| s.borrow().clone());

        if let Some(state) = shared_state {
            // Use shared state - all instances will share the same fault config
            Ok(Self { inner, state })
        } else {
            // Normal path: create default config
            Ok(Self::with_seed(inner, 0))
        }
    }

    fn pread(&self, buf: &mut AlignedBuf, offset: u64) -> Self::ReadFuture {
        let inner_future = self.inner.pread(buf, offset);
        FaultyReadFuture {
            inner: inner_future,
            buf: buf as *mut AlignedBuf,
            state: Arc::clone(&self.state),
            latency_applied: false,
        }
    }

    fn pwrite(&self, buf: &AlignedBuf, offset: u64) -> Self::WriteFuture {
        let inner_future = self.inner.pwrite(buf, offset);
        FaultyWriteFuture {
            inner: inner_future,
            buf_size: buf.capacity(),
            state: Arc::clone(&self.state),
            latency_applied: false,
            enospc_checked: false,
        }
    }

    fn fsync(&self) -> impl Future<Output = Result<(), StorageError>> + Send {
        let inner_future = self.inner.fsync();
        FaultyFsyncFuture {
            inner: inner_future,
            state: Arc::clone(&self.state),
            latency_applied: false,
        }
    }

    fn close(self) {
        self.inner.close();
    }
}

impl<IO: DiskIo> FaultyDiskIo<IO> {
    /// Create a FaultyDiskIo with specific config and seed.
    pub fn new(inner: IO, config: DiskFaultConfig, seed: u64) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(FaultState {
                config,
                rng: StdRng::seed_from_u64(seed),
                bytes_written: 0,
            })),
        }
    }

    /// Create a FaultyDiskIo with default config and specific seed.
    pub fn with_seed(inner: IO, seed: u64) -> Self {
        Self::new(inner, DiskFaultConfig::default(), seed)
    }

    /// Set the probability of fsync returning an I/O error.
    pub fn set_fsync_fail_rate(&self, rate: f64) {
        assert!(
            (0.0..=1.0).contains(&rate),
            "fsync_fail_rate must be in [0.0, 1.0]"
        );
        self.state.lock().unwrap().config.fsync_fail_rate = rate;
    }

    /// Set the probability of bit flips on read.
    pub fn set_read_corruption_rate(&self, rate: f64) {
        assert!(
            (0.0..=1.0).contains(&rate),
            "read_corruption_rate must be in [0.0, 1.0]"
        );
        self.state.lock().unwrap().config.read_corruption_rate = rate;
    }

    /// Set ENOSPC threshold in bytes.
    pub fn set_enospc_after_bytes(&self, limit: Option<u64>) {
        self.state.lock().unwrap().config.enospc_after_bytes = limit;
    }

    /// Set I/O latency to inject on all operations.
    pub fn set_slow_io_latency(&self, latency: Option<Duration>) {
        self.state.lock().unwrap().config.slow_io_latency = latency;
    }

    /// Replace the entire fault configuration.
    pub fn set_config(&self, config: DiskFaultConfig) {
        self.state.lock().unwrap().config = config;
    }

    /// Get a copy of the current fault configuration.
    pub fn config(&self) -> DiskFaultConfig {
        self.state.lock().unwrap().config.clone()
    }

    /// Reset the bytes written counter to zero.
    pub fn reset_bytes_written(&self) {
        self.state.lock().unwrap().bytes_written = 0;
    }

    /// Get the total bytes written through this DiskIo instance.
    pub fn bytes_written(&self) -> u64 {
        self.state.lock().unwrap().bytes_written
    }

    /// Reset all fault injection to default (no faults).
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        state.config = DiskFaultConfig::default();
        state.bytes_written = 0;
    }

    /// Enable shared fault state for testing.
    ///
    /// When enabled, all FaultyDiskIo instances created via `open()` in the
    /// current thread will share the same fault state. This allows runtime
    /// configuration changes to affect all instances (e.g., VlogSegment IO
    /// and all LSM SSTable IOs).
    ///
    /// Must call `disable_shared_fault_state()` after testing to clean up.
    pub fn enable_shared_fault_state(config: DiskFaultConfig, seed: u64) {
        SHARED_FAULT_STATE.with(|s| {
            *s.borrow_mut() = Some(Arc::new(Mutex::new(FaultState {
                config,
                rng: StdRng::seed_from_u64(seed),
                bytes_written: 0,
            })));
        });
    }

    /// Get a handle to the shared fault state for runtime modification.
    ///
    /// Returns None if shared state is not enabled. The returned Arc can
    /// be used to modify the fault configuration at runtime, affecting all
    /// FaultyDiskIo instances created in this thread.
    pub fn get_shared_fault_state() -> Option<Arc<Mutex<FaultState>>> {
        SHARED_FAULT_STATE.with(|s| s.borrow().clone())
    }

    /// Disable shared fault state for testing.
    ///
    /// Clears the thread-local shared state. Should be called after testing
    /// to avoid affecting other tests.
    pub fn disable_shared_fault_state() {
        SHARED_FAULT_STATE.with(|s| *s.borrow_mut() = None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::disk::disk_io::BufferedIo;
    use tempfile::NamedTempFile;

    type TestFaultyIo = FaultyDiskIo<BufferedIo>;

    #[tokio::test]
    async fn test_no_faults_with_default_config() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();
        let faulty = FaultyDiskIo::with_seed(io, 42);

        // Default config should never inject faults
        let mut buf = AlignedBuf::new(4096);
        buf.fill(b"test data");

        // Write should succeed
        faulty.pwrite(&buf, 0).await.unwrap();

        // Fsync to ensure data is on disk
        faulty.fsync().await.unwrap();

        // Read should succeed and return correct data
        let mut read_buf = AlignedBuf::new(4096);
        faulty.pread(&mut read_buf, 0).await.unwrap();
        assert_eq!(&read_buf.as_slice()[..9], b"test data");
    }

    #[tokio::test]
    async fn test_fsync_fail_rate_always() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();

        let mut config = DiskFaultConfig::default();
        config.fsync_fail_rate = 1.0; // Always fail

        let faulty = FaultyDiskIo::new(io, config, 42);

        // fsync should always fail
        for _ in 0..5 {
            let result = faulty.fsync().await;
            assert!(result.is_err());
            if let Err(StorageError::Io(e)) = result {
                assert_eq!(e.to_string(), "injected fsync failure");
            } else {
                panic!("Expected StorageError::Io");
            }
        }
    }

    #[tokio::test]
    async fn test_fsync_fail_rate_never() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();

        let mut config = DiskFaultConfig::default();
        config.fsync_fail_rate = 0.0; // Never fail

        let faulty = FaultyDiskIo::new(io, config, 42);

        // fsync should never fail
        for _ in 0..5 {
            faulty.fsync().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_enospc_simulation() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();

        let mut config = DiskFaultConfig::default();
        config.enospc_after_bytes = Some(8192); // Fail after 8KB

        let faulty = FaultyDiskIo::new(io, config, 42);

        let buf = AlignedBuf::new(4096);

        // First write should succeed (0 + 4096 <= 8192)
        faulty.pwrite(&buf, 0).await.unwrap();
        assert_eq!(faulty.bytes_written(), 4096);

        // Second write should succeed (4096 + 4096 <= 8192)
        faulty.pwrite(&buf, 4096).await.unwrap();
        assert_eq!(faulty.bytes_written(), 8192);

        // Third write should fail (8192 + 4096 > 8192)
        let result = faulty.pwrite(&buf, 8192).await;
        assert!(result.is_err());
        if let Err(StorageError::Io(e)) = result {
            assert!(e.to_string().contains("No space left on device"));
        } else {
            panic!("Expected StorageError::Io for ENOSPC");
        }

        // Bytes written should not have increased
        assert_eq!(faulty.bytes_written(), 8192);
    }

    #[tokio::test]
    async fn test_slow_io_latency() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();

        let mut config = DiskFaultConfig::default();
        config.slow_io_latency = Some(Duration::from_millis(50));

        let faulty = FaultyDiskIo::new(io, config, 42);

        let mut buf = AlignedBuf::new(4096);
        let start = std::time::Instant::now();
        faulty.pread(&mut buf, 0).await.unwrap();
        let elapsed = start.elapsed();

        // Should take at least 50ms due to injected latency
        assert!(
            elapsed >= Duration::from_millis(45),
            "Expected at least 45ms, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_runtime_config_mutation() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();
        let faulty = FaultyDiskIo::with_seed(io, 42);

        // Start with no failures
        faulty.set_fsync_fail_rate(0.0);
        assert!(faulty.fsync().await.is_ok());

        // Change to always fail
        faulty.set_fsync_fail_rate(1.0);
        assert!(faulty.fsync().await.is_err());

        // Change back to never fail
        faulty.set_fsync_fail_rate(0.0);
        assert!(faulty.fsync().await.is_ok());
    }

    #[tokio::test]
    async fn test_read_corruption_determinism() {
        // Create two instances with same seed
        let file1 = NamedTempFile::new().unwrap();
        let file2 = NamedTempFile::new().unwrap();

        let io1 = BufferedIo::open(file1.path(), OpenFlags::default()).unwrap();
        let io2 = BufferedIo::open(file2.path(), OpenFlags::default()).unwrap();

        let mut config = DiskFaultConfig::default();
        config.read_corruption_rate = 0.5;

        let faulty1 = FaultyDiskIo::new(io1, config.clone(), 999);
        let faulty2 = FaultyDiskIo::new(io2, config, 999);

        // Write identical data
        let mut buf = AlignedBuf::new(4096);
        buf.fill(b"test data for determinism check");
        faulty1.pwrite(&buf, 0).await.unwrap();
        faulty2.pwrite(&buf, 0).await.unwrap();

        // Read multiple times and collect corruption results
        let mut results1 = Vec::new();
        let mut results2 = Vec::new();

        for _ in 0..10 {
            let mut buf1 = AlignedBuf::new(4096);
            let mut buf2 = AlignedBuf::new(4096);

            faulty1.pread(&mut buf1, 0).await.unwrap();
            faulty2.pread(&mut buf2, 0).await.unwrap();

            // Check if data was corrupted by comparing to original
            results1.push(buf1.as_slice() != buf.as_slice());
            results2.push(buf2.as_slice() != buf.as_slice());
        }

        // Same seed should produce identical corruption pattern
        assert_eq!(
            results1, results2,
            "Corruption patterns diverged with same seed"
        );

        // With 50% corruption rate and 10 reads, expect some corruptions
        let corruption_count = results1.iter().filter(|&&x| x).count();
        assert!(
            corruption_count > 0,
            "Expected some corruptions with 50% rate"
        );
    }

    #[tokio::test]
    async fn test_read_corruption_always() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();

        let mut config = DiskFaultConfig::default();
        config.read_corruption_rate = 1.0; // Always corrupt

        let faulty = FaultyDiskIo::new(io, config, 42);

        // Write clean data
        let mut write_buf = AlignedBuf::new(4096);
        write_buf.fill(b"clean data");
        faulty.pwrite(&write_buf, 0).await.unwrap();

        // Read should be corrupted
        let mut read_buf = AlignedBuf::new(4096);
        faulty.pread(&mut read_buf, 0).await.unwrap();

        // Data should be different due to corruption
        assert_ne!(
            read_buf.as_slice(),
            write_buf.as_slice(),
            "Expected corruption but data matched"
        );
    }

    #[tokio::test]
    async fn test_reset_bytes_written() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();
        let faulty = FaultyDiskIo::with_seed(io, 42);

        let buf = AlignedBuf::new(4096);

        faulty.pwrite(&buf, 0).await.unwrap();
        assert_eq!(faulty.bytes_written(), 4096);

        faulty.reset_bytes_written();
        assert_eq!(faulty.bytes_written(), 0);

        faulty.pwrite(&buf, 4096).await.unwrap();
        assert_eq!(faulty.bytes_written(), 4096);
    }

    #[tokio::test]
    async fn test_reset_clears_all_faults() {
        let file = NamedTempFile::new().unwrap();
        let io = BufferedIo::open(file.path(), OpenFlags::default()).unwrap();
        let faulty = FaultyDiskIo::with_seed(io, 42);

        // Set various faults
        faulty.set_fsync_fail_rate(1.0);
        faulty.set_enospc_after_bytes(Some(8192));

        let buf = AlignedBuf::new(4096);
        // First write should succeed (4096 <= 8192)
        faulty.pwrite(&buf, 0).await.unwrap();
        assert_eq!(faulty.bytes_written(), 4096);

        // Fsync should fail due to fault
        assert!(faulty.fsync().await.is_err());

        // Second write should succeed (4096 + 4096 <= 8192)
        faulty.pwrite(&buf, 4096).await.unwrap();
        assert_eq!(faulty.bytes_written(), 8192);

        // Reset should clear all faults and bytes_written
        faulty.reset();
        assert_eq!(faulty.bytes_written(), 0);

        // Should now work without faults
        faulty.pwrite(&buf, 0).await.unwrap();
        assert_eq!(faulty.bytes_written(), 4096);
        assert!(faulty.fsync().await.is_ok());
    }

    #[tokio::test]
    async fn fuzz_storage_simulator() {
        use crate::mvcc::backend::MvccBackend;
        use crate::mvcc::disk::disk_store::DiskStore;
        use rand::Rng;
        use std::collections::BTreeMap;
        use std::env;
        use std::time::{SystemTime, UNIX_EPOCH};
        use tempfile::TempDir;

        // Use TAPI_TEST_SEED for deterministic reproduction, otherwise random seed
        let seed = env::var("TAPI_TEST_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        println!("seed={}", seed);

        let mut rng = StdRng::seed_from_u64(seed);
        let dir = TempDir::new().unwrap();

        // Generate random fault config
        let config = DiskFaultConfig {
            fsync_fail_rate: rng.gen_range(0.0..0.3),
            read_corruption_rate: rng.gen_range(0.0..0.1),
            enospc_after_bytes: if rng.gen_bool(0.5) {
                Some(rng.gen_range(10_000..100_000))
            } else {
                None
            },
            slow_io_latency: None,
        };

        println!(
            "fault config: fsync_fail_rate={:.3}, read_corruption_rate={:.3}, enospc_after_bytes={:?}",
            config.fsync_fail_rate, config.read_corruption_rate, config.enospc_after_bytes
        );

        // Enable shared fault state for all FaultyDiskIo instances
        FaultyDiskIo::<BufferedIo>::enable_shared_fault_state(config.clone(), seed);
        let fault_handle = FaultyDiskIo::<BufferedIo>::get_shared_fault_state().unwrap();

        // Create DiskStore with FaultyDiskIo
        type TestStore = DiskStore<String, String, u64, FaultyDiskIo<BufferedIo>>;
        let mut store = TestStore::open(dir.path().to_path_buf()).unwrap();

        // Track what should be persisted (writes followed by successful fsync)
        let mut committed_data: BTreeMap<(String, u64), String> = BTreeMap::new();
        let mut pending_writes: Vec<(String, String, u64)> = Vec::new();

        // Generate random operations
        let num_ops = rng.gen_range(50..200);
        println!("executing {} random operations", num_ops);

        for i in 0..num_ops {
            match rng.gen_range(0..4) {
                0 => {
                    // Write operation
                    let key = format!("key{}", rng.gen_range(0..20));
                    let value = format!("value{}", rng.gen_range(0..u32::MAX));
                    let ts = rng.gen_range(1..1000);

                    match store.put(key.clone(), Some(value.clone()), ts) {
                        Ok(_) => {
                            pending_writes.push((key.clone(), value.clone(), ts));
                            if i % 50 == 0 {
                                println!("  op {}: put {} @ {} = {}", i, key, ts, value);
                            }
                        }
                        Err(e) => {
                            // ENOSPC expected
                            if i % 50 == 0 {
                                println!("  op {}: put failed (expected): {:?}", i, e);
                            }
                        }
                    }
                }
                1 => {
                    // Read operation
                    let key = format!("key{}", rng.gen_range(0..20));
                    let _ = store.get(&key); // May fail with corruption or not found
                }
                2 => {
                    // Fsync operation
                    match store.sync() {
                        Ok(_) => {
                            // Successful fsync - commit pending writes
                            if !pending_writes.is_empty() {
                                println!(
                                    "  op {}: sync succeeded, committing {} writes",
                                    i,
                                    pending_writes.len()
                                );
                                for (k, v, ts) in pending_writes.drain(..) {
                                    committed_data.insert((k, ts), v);
                                }
                                // Save manifest after successful sync
                                if store.save_manifest().is_err() {
                                    // Manifest save may fail due to ENOSPC, but data is still synced
                                    println!("  manifest save failed (ENOSPC expected)");
                                }
                            }
                        }
                        Err(_e) => {
                            // Fsync reported failure. FaultyDiskIo performs
                            // the actual fsync before injecting the error, so
                            // data may still be durable (ambiguous outcome).
                            // If pending writes overwrite committed entries,
                            // the committed value is now ambiguous — remove.
                            if !pending_writes.is_empty() {
                                println!(
                                    "  op {}: sync failed (fault injection), {} pending writes ambiguous",
                                    i,
                                    pending_writes.len()
                                );
                                for (k, _v, ts) in &pending_writes {
                                    committed_data.remove(&(k.clone(), *ts));
                                }
                                pending_writes.clear();
                            }
                        }
                    }
                }
                3 => {
                    // Change fault config at runtime
                    let mut state = fault_handle.lock().unwrap();
                    state.config.fsync_fail_rate = rng.gen_range(0.0..0.5);
                    state.config.read_corruption_rate = rng.gen_range(0.0..0.1);
                    if i % 50 == 0 {
                        println!(
                            "  op {}: updated fault rates: fsync={:.3}, corruption={:.3}",
                            i, state.config.fsync_fail_rate, state.config.read_corruption_rate
                        );
                    }
                }
                _ => unreachable!(),
            }
        }

        // Final sync to commit remaining writes
        match store.sync() {
            Ok(_) => {
                println!(
                    "final sync succeeded, committing {} remaining writes",
                    pending_writes.len()
                );
                for (k, v, ts) in pending_writes.drain(..) {
                    committed_data.insert((k, ts), v);
                }
                let _ = store.save_manifest();
            }
            Err(_) => {
                println!("final sync failed, {} pending writes ambiguous", pending_writes.len());
                for (k, _v, ts) in &pending_writes {
                    committed_data.remove(&(k.clone(), *ts));
                }
                pending_writes.clear();
            }
        }

        println!("committed {} total writes", committed_data.len());

        // Crash simulation: drop store
        drop(store);

        // Disable corruption for recovery verification
        {
            let mut state = fault_handle.lock().unwrap();
            state.config.read_corruption_rate = 0.0;
            state.config.fsync_fail_rate = 0.0;
            state.config.enospc_after_bytes = None;
        }

        println!("simulating crash and recovery...");

        // Recovery: reopen store
        let recovered_store = TestStore::open(dir.path().to_path_buf()).unwrap();

        println!("verifying {} committed writes after recovery", committed_data.len());

        // Verify all committed data is present
        for ((key, ts), expected_value) in &committed_data {
            let (actual, actual_ts) = recovered_store.get_at(key, *ts).unwrap();
            assert_eq!(
                actual.as_ref(),
                Some(expected_value),
                "Mismatch for key {} at ts {}: expected {:?}, got {:?}",
                key,
                ts,
                Some(expected_value),
                actual
            );
            assert_eq!(
                actual_ts, *ts,
                "Timestamp mismatch for key {}: expected {}, got {}",
                key, ts, actual_ts
            );
        }

        println!("all invariants verified successfully!");

        // Clean up
        FaultyDiskIo::<BufferedIo>::disable_shared_fault_state();
    }
}
