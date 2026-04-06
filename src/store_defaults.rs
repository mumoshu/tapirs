//! Centralized production store type aliases and factory.
//!
//! All production stores use CombinedStore, which unifies IR and TAPIR
//! storage into a single deduplicated persistent backend.

use crate::tapir::ShardNumber;
use crate::DefaultDiskIo;

// ── Type aliases ───────────────────────────────────────────────────────────

pub type ProductionTapirStore =
    crate::unified::combined::tapir_handle::CombinedTapirHandle<String, String, DefaultDiskIo>;

pub type ProductionIrRecordStore =
    crate::unified::combined::record_handle::CombinedRecordHandle<String, String, DefaultDiskIo>;

// ── Composites ─────────────────────────────────────────────────────────────

pub type ProductionTapirReplica = crate::tapir::Replica<String, String, ProductionTapirStore>;

pub type ProductionIrReplica = crate::ir::Replica<
    ProductionTapirReplica,
    crate::transport::tokio_bitcode_tcp::TcpTransport<ProductionTapirReplica>,
    ProductionIrRecordStore,
>;

// ── App tick ───────────────────────────────────────────────────────────────

type TcpT = crate::transport::tokio_bitcode_tcp::TcpTransport<ProductionTapirReplica>;
type AppTickFn = fn(
    &ProductionTapirReplica,
    &TcpT,
    &crate::ir::Membership<crate::TcpAddress>,
    &mut crate::Rng,
);

/// Returns the app_tick callback for the production replica.
pub fn production_app_tick() -> Option<AppTickFn> {
    Some(ProductionTapirReplica::tick)
}

// ── S3-backed type aliases (always BufferedIo, not DefaultDiskIo) ─────
//
// DefaultDiskIo is MemoryIo in test builds, but S3-backed stores need
// real filesystem I/O to interact with the S3 cache. These aliases use
// BufferedIo explicitly so the S3 factory works in all build configs.

pub type S3BackedTapirStore =
    crate::unified::combined::tapir_handle::CombinedTapirHandle<String, String, crate::mvcc::disk::disk_io::BufferedIo>;

pub type S3BackedIrRecordStore =
    crate::unified::combined::record_handle::CombinedRecordHandle<String, String, crate::mvcc::disk::disk_io::BufferedIo>;

pub type S3BackedTapirReplica = crate::tapir::Replica<String, String, S3BackedTapirStore>;

pub type S3BackedIrReplica = crate::ir::Replica<
    S3BackedTapirReplica,
    crate::transport::tokio_bitcode_tcp::TcpTransport<S3BackedTapirReplica>,
    S3BackedIrRecordStore,
>;

// ── Factory ────────────────────────────────────────────────────────────────

/// Open production stores for a given shard.
///
/// Returns `(tapir_upcalls, ir_record_store)` ready to pass into `IrReplica`.
///
/// Opens a CombinedStore which unifies IR record storage and TAPIR
/// transaction storage into a single deduplicated persistent backend.
pub fn open_production_stores(
    shard: ShardNumber,
    persist_dir: &str,
    shard_id: u32,
    linearizable: bool,
    s3_config: Option<crate::remote_store::config::S3StorageConfig>,
) -> Result<(ProductionTapirReplica, ProductionIrRecordStore), String> {
    use crate::mvcc::disk::disk_io::OpenFlags;
    use crate::unified::combined::CombinedStoreInner;

    let base_dir = format!("{}/shard_{}", persist_dir, shard_id);
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };

    let mut inner = CombinedStoreInner::<String, String, DefaultDiskIo>::open(
        std::path::Path::new(&base_dir),
        io_flags,
        shard,
        linearizable,
    )
    .map_err(|e| format!("failed to open CombinedStore at {base_dir}: {e}"))?;

    if let Some(cfg) = s3_config {
        inner.set_s3_config(cfg);
    }

    let record_handle = inner.into_record_handle();
    let tapir_handle = record_handle.tapir_handle();
    let upcalls = crate::tapir::Replica::new_with_store(tapir_handle);

    Ok((upcalls, record_handle))
}

/// Open production stores pre-populated from S3 via zero-copy clone.
///
/// Downloads the source manifest and registers S3CachingIo so segments
/// are lazy-downloaded on first read. Then opens the store normally —
/// DefaultDiskIo falls through to S3CachingIo for missing files.
///
/// `source_s3`: where to read source manifests/segments from.
/// `dest_s3`: optional S3 config for the new cluster's ongoing uploads
///            (must be a different bucket/prefix from source).
/// Open production stores pre-populated from S3 via zero-copy clone.
///
/// Downloads the source manifest and registers S3CachingIo so segments
/// are lazy-downloaded on first read. Opens the store with `S3CachingIo`
/// as the DiskIo backend (not `DefaultDiskIo`), which checks the
/// process-global S3 cache registry and downloads missing segments.
///
/// `source_s3`: where to read source manifests/segments from.
/// `dest_s3`: optional S3 config for the new cluster's ongoing uploads
///            (must be a different bucket/prefix from source).
pub fn open_production_stores_from_s3(
    shard: ShardNumber,
    persist_dir: &str,
    shard_id: u32,
    linearizable: bool,
    source_s3: &crate::remote_store::config::S3StorageConfig,
    dest_s3: Option<crate::remote_store::config::S3StorageConfig>,
) -> Result<(S3BackedTapirReplica, S3BackedIrRecordStore), String> {
    use crate::backup::s3backup::S3BackupStorage;
    use crate::backup::storage::BackupStorage;
    use crate::mvcc::disk::disk_io::OpenFlags;
    use crate::remote_store::cow_clone::clone_from_remote_lazy;
    use crate::remote_store::manifest_store::RemoteManifestStore;
    use crate::unified::combined::CombinedStoreInner;

    let base_dir = format!("{}/shard_{}", persist_dir, shard_id);
    let shard_name = format!("shard_{}", shard_id);

    // Clone from S3: download manifest, register S3CachingIo (blocking).
    let handle = tokio::runtime::Handle::current();
    handle.block_on(async {
        let storage = S3BackupStorage::new(
            &source_s3.bucket,
            &source_s3.prefix,
            source_s3.region.as_deref(),
            source_s3.endpoint_url.as_deref(),
        )
        .await;
        let man_store = RemoteManifestStore::new(storage.sub(""));
        clone_from_remote_lazy(
            &man_store,
            source_s3,
            &shard_name,
            None, // latest manifest
            std::path::Path::new(&base_dir),
        )
        .await
    })
    .map_err(|e| format!("clone_from_remote_lazy for {shard_name}: {e}"))?;

    // Open store with BufferedIo (not DefaultDiskIo which is MemoryIo in
    // tests). BufferedIo::open() transparently checks the S3 cache registry
    // and downloads missing segments.
    let io_flags = OpenFlags { create: true, direct: false };
    let mut inner = CombinedStoreInner::<String, String, crate::mvcc::disk::disk_io::BufferedIo>::open(
        std::path::Path::new(&base_dir),
        io_flags,
        shard,
        linearizable,
    )
    .map_err(|e| format!("open CombinedStore at {base_dir}: {e}"))?;

    // Configure destination S3 for ongoing uploads (optional, may differ from source).
    if let Some(cfg) = dest_s3 {
        inner.set_s3_config(cfg);
    }

    let record_handle = inner.into_record_handle();
    let tapir_handle = record_handle.tapir_handle();
    let upcalls = crate::tapir::Replica::new_with_store(tapir_handle);

    Ok((upcalls, record_handle))
}
