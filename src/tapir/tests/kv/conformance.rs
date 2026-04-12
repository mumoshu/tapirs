use crate::discovery::InMemoryShardDirectory;
use crate::storage::io::disk_io::OpenFlags;
use crate::storage::io::memory_io::MemoryIo;
use crate::storage::combined::CombinedStoreInner;
use crate::storage::combined::record_handle::CombinedRecordHandle;
use crate::storage::combined::tapir_handle::CombinedTapirHandle;
use crate::{tapir, ChannelRegistry};
use std::sync::Arc;

type S = CombinedTapirHandle<String, String, MemoryIo>;
type U = tapir::Replica<String, String, S>;

fn combined_factory() -> (
    ChannelRegistry<U>,
    Arc<InMemoryShardDirectory<usize>>,
    impl FnMut() -> (U, CombinedRecordHandle<String, String, MemoryIo>),
) {
    let registry = ChannelRegistry::default();
    let directory = Arc::new(InMemoryShardDirectory::new());
    let factory = || {
        let io_flags = OpenFlags {
            create: true,
            direct: false,
        };
        let inner = CombinedStoreInner::<String, String, MemoryIo>::open(
            &MemoryIo::temp_path(),
            io_flags,
            crate::ShardNumber(0),
            true,
        )
        .unwrap();
        let record_handle = inner.into_record_handle();
        let tapir_handle = record_handle.tapir_handle();
        let upcalls = tapir::Replica::new_with_store(tapir_handle);
        (upcalls, record_handle)
    };
    (registry, directory, factory)
}

crate::ir_replica_conformance_tests!(combined_factory);
