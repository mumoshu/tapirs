//! Centralized production store type aliases and factory.
//!
//! This is the **only** file with `cfg(feature = "persistent-store")`
//! and `cfg(feature = "combined-store")` gates.
//! Node code uses these aliases and the factory function with no cfg of its own.

use crate::tapir::ShardNumber;
#[cfg(not(feature = "combined-store"))]
use crate::tapir::{CO, CR, IO};
use crate::DefaultDiskIo;

// ── Non-persistent (default) ────────────────────────────────────────────────

#[cfg(not(any(feature = "persistent-store", feature = "combined-store")))]
pub type ProductionTapirStore =
    crate::tapirstore::InMemTapirStore<String, String, crate::MvccDiskStore<String, String, crate::tapir::Timestamp, DefaultDiskIo>>;

#[cfg(not(any(feature = "persistent-store", feature = "combined-store")))]
pub type ProductionIrRecordStore =
    crate::ir::VersionedRecord<IO<String, String>, CO<String, String>, CR>;

// ── Persistent ──────────────────────────────────────────────────────────────

#[cfg(feature = "persistent-store")]
pub type ProductionTapirStore =
    crate::unified::tapir::persistent_store::PersistentTapirStore<String, String, DefaultDiskIo>;

#[cfg(feature = "persistent-store")]
pub type ProductionIrRecordStore =
    crate::unified::ir::ir_record_store::PersistentIrRecordStore<
        IO<String, String>,
        CO<String, String>,
        CR,
        DefaultDiskIo,
    >;

// ── Combined (deduplicated IR+TAPIR) ────────────────────────────────────────

#[cfg(feature = "combined-store")]
pub type ProductionTapirStore =
    crate::unified::combined::tapir_handle::CombinedTapirHandle<String, String, DefaultDiskIo>;

#[cfg(feature = "combined-store")]
pub type ProductionIrRecordStore =
    crate::unified::combined::record_handle::CombinedRecordHandle<String, String, DefaultDiskIo>;

// ── Composites (feature-independent) ────────────────────────────────────────

pub type ProductionTapirReplica = crate::tapir::Replica<String, String, ProductionTapirStore>;

pub type ProductionIrReplica = crate::ir::Replica<
    ProductionTapirReplica,
    crate::transport::tokio_bitcode_tcp::TcpTransport<ProductionTapirReplica>,
    ProductionIrRecordStore,
>;

// ── App tick ────────────────────────────────────────────────────────────────

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

// ── Factory ─────────────────────────────────────────────────────────────────

/// Open production stores for a given shard.
///
/// Returns `(tapir_upcalls, ir_record_store)` ready to pass into `IrReplica`.
///
/// Non-persistent path: opens `MvccDiskStore` + `InMemTapirStore`, returns
/// default `IrVersionedRecord`.
///
/// Persistent path: opens `TapirState` + `PersistentTapirStore` and
/// `PersistentIrRecordStore` from disk.
#[cfg(not(any(feature = "persistent-store", feature = "combined-store")))]
pub fn open_production_stores(
    shard: ShardNumber,
    persist_dir: &str,
    shard_id: u32,
) -> Result<(ProductionTapirReplica, ProductionIrRecordStore), String> {
    let mvcc_dir = format!("{}/shard_{}/mvcc", persist_dir, shard_id);

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    let backend = crate::MvccDiskStore::open_with_flags(
        std::path::PathBuf::from(&mvcc_dir),
        crate::DiskOpenFlags {
            create: true,
            direct: true,
        },
    )
    .map_err(|e| format!("failed to open DiskStore at {mvcc_dir}: {e}"))?;

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    let backend = crate::MvccDiskStore::open(std::path::PathBuf::from(&mvcc_dir))
        .map_err(|e| format!("failed to open DiskStore at {mvcc_dir}: {e}"))?;

    let upcalls = crate::TapirReplica::new_with_backend(shard, false, backend);
    let ir_store = ProductionIrRecordStore::default();
    Ok((upcalls, ir_store))
}

#[cfg(feature = "persistent-store")]
pub fn open_production_stores(
    shard: ShardNumber,
    persist_dir: &str,
    shard_id: u32,
) -> Result<(ProductionTapirReplica, ProductionIrRecordStore), String> {
    use crate::mvcc::disk::disk_io::OpenFlags;
    use crate::unified::tapir::persistent_store::PersistentTapirStore;

    let base_dir = format!("{}/shard_{}", persist_dir, shard_id);
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };

    let state = crate::unified::tapir::store::open::<String, String, DefaultDiskIo>(
        std::path::Path::new(&base_dir),
        io_flags,
        shard,
        true,
    )
    .map_err(|e| format!("failed to open TapirState at {base_dir}: {e}"))?;

    let store = PersistentTapirStore::new(state);
    let upcalls = crate::tapir::Replica::new_with_store(store);

    let ir_store = ProductionIrRecordStore::open(
        std::path::Path::new(&base_dir),
        io_flags,
    )
    .map_err(|e| format!("failed to open PersistentIrRecordStore at {base_dir}: {e}"))?;

    Ok((upcalls, ir_store))
}

#[cfg(feature = "combined-store")]
pub fn open_production_stores(
    shard: ShardNumber,
    persist_dir: &str,
    shard_id: u32,
) -> Result<(ProductionTapirReplica, ProductionIrRecordStore), String> {
    use crate::mvcc::disk::disk_io::OpenFlags;
    use crate::unified::combined::CombinedStoreInner;

    let base_dir = format!("{}/shard_{}", persist_dir, shard_id);
    let io_flags = OpenFlags {
        create: true,
        direct: false,
    };

    let inner = CombinedStoreInner::<String, String, DefaultDiskIo>::open(
        std::path::Path::new(&base_dir),
        io_flags,
        shard,
        true,
    )
    .map_err(|e| format!("failed to open CombinedStore at {base_dir}: {e}"))?;

    let record_handle = inner.into_record_handle();
    let tapir_handle = record_handle.tapir_handle();
    let upcalls = crate::tapir::Replica::new_with_store(tapir_handle);

    Ok((upcalls, record_handle))
}
