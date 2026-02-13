#[cfg(test)]
mod tests {
    use crate::mvcc::backend::MvccBackend;
    use crate::mvcc::disk::disk_io::BufferedIo;
    use crate::mvcc::disk::disk_store::DiskStore;
    use tempfile::TempDir;

    type TestStore = DiskStore<String, String, u64, BufferedIo>;

    fn open_store() -> (TempDir, TestStore) {
        let dir = TempDir::new().unwrap();
        let store = TestStore::open(dir.path().to_path_buf()).unwrap();
        (dir, store)
    }

    #[test]
    fn get_nonexistent_key() {
        let (_dir, store) = open_store();
        let (v, ts): (Option<String>, u64) = MvccBackend::get(&store, &"missing".into()).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 0);
    }

    #[test]
    fn put_and_get() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "k1".into(), Some("v1".into()), 10).unwrap();

        let (v, ts) = MvccBackend::get(&store, &"k1".into()).unwrap();
        assert_eq!(v, Some("v1".into()));
        assert_eq!(ts, 10);
    }

    #[test]
    fn multiple_keys() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "a".into(), Some("1".into()), 5).unwrap();
        MvccBackend::put(&mut store, "b".into(), Some("2".into()), 6).unwrap();
        MvccBackend::put(&mut store, "c".into(), Some("3".into()), 7).unwrap();

        assert_eq!(MvccBackend::get(&store, &"a".into()).unwrap().0, Some("1".into()));
        assert_eq!(MvccBackend::get(&store, &"b".into()).unwrap().0, Some("2".into()));
        assert_eq!(MvccBackend::get(&store, &"c".into()).unwrap().0, Some("3".into()));
    }

    #[test]
    fn get_at_returns_correct_version() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "k".into(), Some("v1".into()), 10).unwrap();
        MvccBackend::put(&mut store, "k".into(), Some("v2".into()), 20).unwrap();
        MvccBackend::put(&mut store, "k".into(), Some("v3".into()), 30).unwrap();

        // Before any version.
        let (v, ts) = MvccBackend::get_at(&store, &"k".into(), 5).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 0);

        // At exact version timestamps.
        let (v, ts) = MvccBackend::get_at(&store, &"k".into(), 10).unwrap();
        assert_eq!(v, Some("v1".into()));
        assert_eq!(ts, 10);

        let (v, ts) = MvccBackend::get_at(&store, &"k".into(), 20).unwrap();
        assert_eq!(v, Some("v2".into()));
        assert_eq!(ts, 20);

        // Between versions.
        let (v, ts) = MvccBackend::get_at(&store, &"k".into(), 15).unwrap();
        assert_eq!(v, Some("v1".into()));
        assert_eq!(ts, 10);

        // After all versions.
        let (v, ts) = MvccBackend::get_at(&store, &"k".into(), 100).unwrap();
        assert_eq!(v, Some("v3".into()));
        assert_eq!(ts, 30);
    }

    #[test]
    fn tombstone_deletes_key() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "k".into(), Some("v1".into()), 10).unwrap();
        MvccBackend::put(&mut store, "k".into(), None, 20).unwrap();

        // Latest version is tombstone.
        let (v, ts) = MvccBackend::get(&store, &"k".into()).unwrap();
        assert_eq!(v, None);
        assert_eq!(ts, 20);

        // Old version still accessible.
        let (v, ts) = MvccBackend::get_at(&store, &"k".into(), 15).unwrap();
        assert_eq!(v, Some("v1".into()));
        assert_eq!(ts, 10);
    }

    #[test]
    fn get_range_basic() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "k".into(), Some("v1".into()), 10).unwrap();
        MvccBackend::put(&mut store, "k".into(), Some("v2".into()), 20).unwrap();

        let (start, end) = MvccBackend::get_range(&store, &"k".into(), 15).unwrap();
        assert_eq!(start, 10);
        assert_eq!(end, Some(20));
    }

    #[test]
    fn commit_get_and_last_read() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "k".into(), Some("v1".into()), 10).unwrap();

        // Initially no last-read.
        let lr = MvccBackend::get_last_read(&store, &"k".into()).unwrap();
        assert_eq!(lr, None);

        // Record a read.
        MvccBackend::commit_get(&mut store, "k".into(), 10, 50).unwrap();

        let lr = MvccBackend::get_last_read(&store, &"k".into()).unwrap();
        assert!(lr.is_some());
    }

    #[test]
    fn many_puts_triggers_flush() {
        let (_dir, mut store) = open_store();

        // Write enough data to trigger memtable flush (64KiB threshold).
        for i in 0u64..2000 {
            let key = format!("key-{i:06}");
            let val = format!("value-{i:06}-padding-to-make-it-bigger-{}", "x".repeat(20));
            MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
        }

        // Data should still be accessible (either from memtable or SSTable).
        let (v, ts) = MvccBackend::get(&store, &"key-000000".into()).unwrap();
        assert_eq!(ts, 1);
        assert!(v.is_some());

        let (v, ts) = MvccBackend::get(&store, &"key-001999".into()).unwrap();
        assert_eq!(ts, 2000);
        assert!(v.is_some());
    }

    #[test]
    fn scan_basic() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "a".into(), Some("1".into()), 5).unwrap();
        MvccBackend::put(&mut store, "b".into(), Some("2".into()), 5).unwrap();
        MvccBackend::put(&mut store, "c".into(), Some("3".into()), 5).unwrap();
        MvccBackend::put(&mut store, "d".into(), Some("4".into()), 5).unwrap();

        let results = MvccBackend::scan(&store, &"b".into(), &"c".into(), 10).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "b");
        assert_eq!(results[1].0, "c");
    }

    #[test]
    fn has_writes_in_range_check() {
        let (_dir, mut store) = open_store();
        MvccBackend::put(&mut store, "a".into(), Some("1".into()), 10).unwrap();
        MvccBackend::put(&mut store, "b".into(), Some("2".into()), 20).unwrap();

        // Should find write at ts=10 in range (5, 15).
        let has = MvccBackend::has_writes_in_range(
            &store, &"a".into(), &"b".into(), 5, 15,
        )
        .unwrap();
        assert!(has);

        // Should not find writes in range (25, 35).
        let has = MvccBackend::has_writes_in_range(
            &store, &"a".into(), &"b".into(), 25, 35,
        )
        .unwrap();
        assert!(!has);
    }

    #[test]
    fn reopen_after_flush() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Write enough to flush, then save manifest.
        {
            let mut store = TestStore::open(path.clone()).unwrap();
            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }
            store.save_manifest().unwrap();
        }

        // Reopen and verify flushed data is accessible from SSTables.
        {
            let store = TestStore::open(path).unwrap();
            // After reopening, memtable is empty but SSTables contain
            // the flushed data.
            let (v, ts) = MvccBackend::get(&store, &"key-000000".into()).unwrap();
            assert_eq!(ts, 1);
            assert!(v.is_some());
        }
    }
}
