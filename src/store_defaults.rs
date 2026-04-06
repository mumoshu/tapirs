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
