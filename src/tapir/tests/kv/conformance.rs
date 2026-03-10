use crate::discovery::InMemoryShardDirectory;
use crate::mvcc::disk::{DiskStore, memory_io::MemoryIo};
use crate::tapir::{self, Timestamp};
use crate::{ChannelRegistry, IrVersionedRecord};
use std::sync::Arc;

type S = crate::tapir::store::InMemTapirStore<
    String,
    String,
    DiskStore<String, String, Timestamp, MemoryIo>,
>;
type U = tapir::Replica<String, String, S>;

fn inmem_factory() -> (
    ChannelRegistry<U>,
    Arc<InMemoryShardDirectory<usize>>,
    impl FnMut() -> (U, IrVersionedRecord<tapir::IO<String, String>, tapir::CO<String, String>, tapir::CR>),
) {
    let registry = ChannelRegistry::default();
    let directory = Arc::new(InMemoryShardDirectory::new());
    let factory = || {
        let backend =
            DiskStore::<String, String, Timestamp, MemoryIo>::open(MemoryIo::temp_path()).unwrap();
        let upcalls = tapir::Replica::new_with_backend(
            crate::ShardNumber(0),
            true,
            backend,
        );
        (upcalls, IrVersionedRecord::default())
    };
    (registry, directory, factory)
}

crate::ir_replica_conformance_tests!(inmem_factory);
