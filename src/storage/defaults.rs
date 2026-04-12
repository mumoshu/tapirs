//! Centralized production store type aliases and factory.
//!
//! All production stores use CombinedStore, which unifies IR and TAPIR
//! storage into a single deduplicated persistent backend.

use crate::tapir::ShardNumber;
use crate::DefaultDiskIo;

// ── Type aliases ───────────────────────────────────────────────────────────

pub type ProductionTapirStore =
    crate::storage::combined::tapir_handle::CombinedTapirHandle<String, String, DefaultDiskIo>;

pub type ProductionIrRecordStore =
    crate::storage::combined::record_handle::CombinedRecordHandle<String, String, DefaultDiskIo>;

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

// S3-backed stores use BufferedIo explicitly because DefaultDiskIo is
// MemoryIo in test builds (#[cfg(test)]), which bypasses the filesystem
// and the S3 cache registry. BufferedIo::open() checks the process-global
// S3CachingIo registry and downloads missing segments — MemoryIo does not.

type S3TcpT = crate::transport::tokio_bitcode_tcp::TcpTransport<S3BackedTapirReplica>;
type S3AppTickFn = fn(
    &S3BackedTapirReplica,
    &S3TcpT,
    &crate::ir::Membership<crate::TcpAddress>,
    &mut crate::Rng,
);

/// Returns the app_tick callback for S3-backed replicas.
pub fn s3_backed_app_tick() -> Option<S3AppTickFn> {
    Some(S3BackedTapirReplica::tick)
}

// ── S3-backed type aliases (always BufferedIo, not DefaultDiskIo) ─────
//
// DefaultDiskIo is MemoryIo in test builds, but S3-backed stores need
// real filesystem I/O to interact with the S3 cache. These aliases use
// BufferedIo explicitly so the S3 factory works in all build configs.

pub type S3BackedTapirStore =
    crate::storage::combined::tapir_handle::CombinedTapirHandle<String, String, crate::storage::io::disk_io::BufferedIo>;

pub type S3BackedIrRecordStore =
    crate::storage::combined::record_handle::CombinedRecordHandle<String, String, crate::storage::io::disk_io::BufferedIo>;

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
    s3_config: Option<crate::storage::remote::config::S3StorageConfig>,
    cluster_type: &str,
) -> Result<(ProductionTapirReplica, ProductionIrRecordStore), String> {
    use crate::storage::io::disk_io::OpenFlags;
    use crate::storage::combined::CombinedStoreInner;

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

    inner.tapir_manifest.cluster_type = cluster_type.to_string();

    if let Some(cfg) = s3_config {
        inner.set_s3_config(cfg);
    }

    let record_handle = inner.into_record_handle();
    let tapir_handle = record_handle.tapir_handle();
    let upcalls = crate::tapir::Replica::new_with_store(tapir_handle);

    Ok((upcalls, record_handle))
}

/// Open production stores pre-populated from a cross-shard S3 snapshot.
///
/// Uses the `CrossShardSnapshot` to determine the exact manifest view and
/// ghost filter for this shard, ensuring cross-shard consistency. Downloads
/// the manifest via `clone_from_remote_lazy(view)` and registers S3CachingIo
/// for lazy segment downloads. Opens the store with BufferedIo (not
/// DefaultDiskIo which is MemoryIo in tests).
///
/// After opening, applies the ghost filter and removes prepared transactions
/// within the ghost range. See `remove_prepared_txns_in_ghost_range` for why
/// this is safe.
///
/// `source_s3`: S3 config for the source cluster's bucket.
/// `snapshot`: cross-shard consistent snapshot (provides per-shard view + ghost filter).
/// `dest_s3`: optional S3 config for the clone's ongoing uploads (must be a
///            different bucket/prefix from source).
pub fn open_production_stores_from_s3(
    shard: ShardNumber,
    persist_dir: &str,
    shard_id: u32,
    linearizable: bool,
    source_s3: &crate::storage::remote::config::S3StorageConfig,
    snapshot: &crate::storage::remote::cross_shard_snapshot::CrossShardSnapshot,
    dest_s3: Option<crate::storage::remote::config::S3StorageConfig>,
) -> Result<(S3BackedTapirReplica, S3BackedIrRecordStore), String> {
    use crate::backup::s3backup::S3BackupStorage;
    use crate::backup::storage::BackupStorage;
    use crate::storage::io::disk_io::OpenFlags;
    use crate::storage::remote::cow_clone::clone_from_remote_lazy;
    use crate::storage::remote::manifest_store::RemoteManifestStore;
    use crate::storage::combined::CombinedStoreInner;

    let base_dir = format!("{}/shard_{}", persist_dir, shard_id);
    let shard_name = format!("shard_{}", shard_id);

    // Get the exact manifest view for this shard from the snapshot.
    let shard_info = snapshot.shards.get(&shard_id).ok_or_else(|| {
        format!("shard {shard_id} not found in CrossShardSnapshot")
    })?;
    let view = shard_info.manifest_view;

    // Clone from S3 at the specific view (blocking).
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
            view,
            std::path::Path::new(&base_dir),
        )
        .await
    })
    .map_err(|e| format!("clone_from_remote_lazy for {shard_name} at view {view}: {e}"))?;

    // Open store with BufferedIo.
    let io_flags = OpenFlags { create: true, direct: false };
    let mut inner = CombinedStoreInner::<String, String, crate::storage::io::disk_io::BufferedIo>::open(
        std::path::Path::new(&base_dir),
        io_flags,
        shard,
        linearizable,
    )
    .map_err(|e| format!("open CombinedStore at {base_dir}: {e}"))?;

    eprintln!(
        "[clone_open] shard={shard_id} view={} ir_inc_active_id={} ir_inc_active_offset={} ir_inc_sealed={}",
        inner.tapir_manifest.current_view,
        inner.tapir_manifest.ir_inc.active_segment_id,
        inner.tapir_manifest.ir_inc.active_write_offset,
        inner.tapir_manifest.ir_inc.sealed_vlog_segments.len(),
    );
    inner.ir.log_ir_inc_state("clone_open");

    // Apply ghost filter for cross-shard consistency.
    if let Some(gf) = snapshot.ghost_filter() {
        inner.ghost_filter = Some(gf);
    }

    // Configure destination S3 for ongoing uploads (optional).
    if let Some(cfg) = dest_s3 {
        inner.set_s3_config(cfg);
    }

    let record_handle = inner.into_record_handle();
    let mut tapir_handle = record_handle.tapir_handle();

    // Remove prepared transactions within the ghost filter range.
    // These are in the hidden range (cutoff_ts, ceiling_ts] — the ghost
    // filter makes this range invisible to the clone, so these prepared
    // transactions can never be meaningfully committed. See the doc comment
    // on remove_prepared_txns_in_ghost_range for full safety argument.
    tapir_handle.remove_prepared_txns_in_ghost_range(
        snapshot.cutoff_ts,
        snapshot.ceiling_ts,
    );

    let upcalls = crate::tapir::Replica::new_with_store(tapir_handle);

    Ok((upcalls, record_handle))
}
