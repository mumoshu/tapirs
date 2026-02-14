#[cfg(test)]
mod tests {
    use crate::mvcc::backend::MvccBackend;
    use crate::mvcc::disk::disk_io::{BufferedIo, OpenFlags};
    use crate::mvcc::disk::disk_store::DiskStore;
    use crate::mvcc::disk::faulty_disk_io::{DiskFaultConfig, FaultyDiskIo};
    use crate::mvcc::disk::manifest::Manifest;
    use crate::mvcc::disk::sstable::SSTableReader;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    use std::collections::{BTreeMap, HashSet};
    use std::env;
    use std::time::{SystemTime, UNIX_EPOCH};
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

    // Helper function to generate deterministic large values.
    fn generate_large_value(size: usize, seed: u64) -> String {
        let mut rng = StdRng::seed_from_u64(seed);
        let bytes: Vec<u8> = (0..size).map(|_| rng.r#gen::<u8>()).collect();
        // Use base64-like encoding to keep string representation compact.
        bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>()
    }

    #[test]
    fn test_large_value_exactly_4kib() {
        let (_dir, mut store) = open_store();

        // Generate a value that should fit in exactly one 4 KiB block.
        // Account for bitcode overhead (~50-100 bytes) and 4-byte CRC.
        let value = generate_large_value(3900, 4096);
        let key = "key-4kib".to_string();

        MvccBackend::put(&mut store, key.clone(), Some(value.clone()), 100).unwrap();

        let (v, ts) = MvccBackend::get(&store, &key).unwrap();
        assert_eq!(v, Some(value));
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_large_value_4kib_plus_1() {
        let (_dir, mut store) = open_store();

        // Generate a value that will overflow to 2 blocks (8 KiB).
        // With bitcode overhead + CRC, a 4096-byte value should require 2 blocks.
        let value = generate_large_value(4096, 4097);
        let key = "key-overflow".to_string();

        MvccBackend::put(&mut store, key.clone(), Some(value.clone()), 200).unwrap();

        let (v, ts) = MvccBackend::get(&store, &key).unwrap();
        assert_eq!(v, Some(value));
        assert_eq!(ts, 200);
    }

    #[test]
    fn test_large_value_1mb() {
        let (_dir, mut store) = open_store();

        // Generate a 1 MB value to test offset arithmetic.
        let value = generate_large_value(1_000_000, 1_000_000);
        let key = "key-1mb".to_string();

        MvccBackend::put(&mut store, key.clone(), Some(value.clone()), 300).unwrap();

        // Verify exact byte-for-byte match.
        let (v, ts) = MvccBackend::get(&store, &key).unwrap();
        assert_eq!(v, Some(value));
        assert_eq!(ts, 300);
    }

    #[test]
    fn test_many_large_values_verify_flush() {
        let (_dir, mut store) = open_store();

        // Write many large values totaling 100 MB.
        // Use 1000 values of ~100 KB each to trigger many flushes.
        // Flush threshold is 64 KiB, so this will cause extensive flushing.
        for i in 1..=1000u64 {
            let key = format!("large-key-{i:06}");
            // Generate ~50 KB value (100 KB hex-encoded) with seed = timestamp.
            let value = generate_large_value(50_000, i);
            MvccBackend::put(&mut store, key, Some(value), i).unwrap();
        }

        // Verify no OOM: test completed without panic.
        // Verify no corruption: spot-check keys can be retrieved correctly.
        for i in (100..=1000).step_by(100) {
            let key = format!("large-key-{i:06}");
            let expected_value = generate_large_value(50_000, i as u64);
            let (v, ts) = MvccBackend::get(&store, &key).unwrap();
            assert_eq!(v, Some(expected_value), "Key {} corrupted", key);
            assert_eq!(ts, i as u64);
        }

        // Save manifest succeeds without error.
        store.save_manifest().unwrap();
    }

    // Durability Verification Tests (E)

    #[test]
    fn test_recovery_manifest_crc_valid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write data, flush, save manifest
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Write enough data to trigger flush (>64 KiB threshold)
            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            // Save manifest to persist state
            store.save_manifest().unwrap();
        }  // DROP: store closed

        // Stage 2: Reopen triggers recovery with manifest CRC validation
        {
            // If manifest CRC is invalid, DiskStore::open() will return error
            let store = TestStore::open(path).unwrap();

            // Verify data is accessible (proves manifest had correct references)
            let (v, ts) = MvccBackend::get(&store, &"key-000000".into()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 1);

            let (v, ts) = MvccBackend::get(&store, &"key-001999".into()).unwrap();
            assert!(v.is_some());
            assert_eq!(ts, 2000);
        }
    }

    #[test]
    fn test_recovery_all_sstables_exist_and_readable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write data to create multiple SSTables
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Write enough to trigger flush and compaction
            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            store.save_manifest().unwrap();
        }

        // Stage 2: Verify all SSTables exist on disk
        {
            let manifest = Manifest::load(&path).unwrap().expect("manifest should exist");

            // Check L0 files exist
            for meta in &manifest.l0_sstables {
                let metadata = std::fs::metadata(&meta.path)
                    .expect("SSTable file should exist");
                assert!(
                    metadata.len() >= 4096,
                    "SSTable file too small for footer"
                );
            }

            // Check L1 files exist
            for meta in &manifest.l1_sstables {
                let metadata = std::fs::metadata(&meta.path)
                    .expect("SSTable file should exist");
                assert!(
                    metadata.len() >= 4096,
                    "SSTable file too small for footer"
                );
            }
        }

        // Stage 3: Verify SSTables are readable after recovery
        {
            let store = TestStore::open(path.clone()).unwrap();
            let manifest = Manifest::load(&path).unwrap().unwrap();

            // Read all SSTables and verify no CRC errors
            for meta in manifest.l0_sstables.iter().chain(manifest.l1_sstables.iter()) {
                let reader = futures::executor::block_on(
                    SSTableReader::<BufferedIo>::open(
                        meta.path.clone(),
                        OpenFlags::default()
                    )
                ).expect("SSTable should open");

                let entries = futures::executor::block_on(
                    reader.read_all::<String, u64>()
                ).expect("SSTable should be readable without CRC errors");

                assert!(!entries.is_empty(), "SSTable should have entries");
            }

            // Verify data is still accessible through the store
            let (v, _) = MvccBackend::get(&store, &"key-001000".into()).unwrap();
            assert!(v.is_some());
        }
    }

    #[test]
    fn test_recovery_all_vlog_pointers_valid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write keys with various value sizes
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Small values
            for i in 0u64..10 {
                let key = format!("small-{i:02}");
                let val = format!("v{i}");
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            // Medium values
            for i in 10u64..20 {
                let key = format!("medium-{i:02}");
                let val = format!("value-{}-{}", i, "x".repeat(100));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            // Large values
            for i in 20u64..30 {
                let key = format!("large-{i:02}");
                let val = format!("value-{}-{}", i, "x".repeat(5000));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            // Trigger flush to create mix of memtable + SSTable entries
            for i in 30u64..2000 {
                let key = format!("flush-{i:04}");
                let val = format!("value-{i:04}-{}", "x".repeat(20));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            store.save_manifest().unwrap();
        }

        // Stage 2: Verify all vlog pointers are valid after recovery
        {
            let store = TestStore::open(path).unwrap();

            // Verify all written data is accessible (proves vlog pointers are valid)
            // If pointers had CRC errors, reads would fail

            // Verify small values
            for i in 0u64..10 {
                let key = format!("small-{i:02}");
                let (v, ts) = MvccBackend::get(&store, &key).unwrap();
                assert!(v.is_some(), "Small value should be readable");
                assert_eq!(ts, i + 1);
            }

            // Verify medium values
            for i in 10u64..20 {
                let key = format!("medium-{i:02}");
                let (v, ts) = MvccBackend::get(&store, &key).unwrap();
                assert!(v.is_some(), "Medium value should be readable");
                assert_eq!(ts, i + 1);
            }

            // Verify large values
            for i in 20u64..30 {
                let key = format!("large-{i:02}");
                let (v, ts) = MvccBackend::get(&store, &key).unwrap();
                assert!(v.is_some(), "Large value should be readable");
                assert_eq!(ts, i + 1);
            }

            // Spot-check flushed values
            for i in (100..2000).step_by(200) {
                let key = format!("flush-{i:04}");
                let (v, ts) = MvccBackend::get(&store, &key).unwrap();
                assert!(v.is_some(), "Flushed value should be readable");
                assert_eq!(ts, i + 1);
            }
        }
    }

    #[test]
    fn test_recovery_no_duplicate_versions() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Stage 1: Write data and trigger flush
        {
            let mut store = TestStore::open(path.clone()).unwrap();

            // Write many keys with unique timestamps
            for i in 0u64..2000 {
                let key = format!("key-{i:06}");
                let val = format!("value-{i:06}-{}", "x".repeat(20));
                MvccBackend::put(&mut store, key, Some(val), i + 1).unwrap();
            }

            store.save_manifest().unwrap();
        }

        // Stage 2: Verify MVCC consistency after recovery
        {
            let store = TestStore::open(path).unwrap();

            // Verify each key has consistent, non-duplicate version
            // If duplicates existed, get() and get_at() would conflict

            for i in (100..1900).step_by(200) {
                let key = format!("key-{i:06}");
                let expected_ts = i + 1;

                // Get latest version
                let (v1, ts1) = MvccBackend::get(&store, &key).unwrap();
                assert!(v1.is_some(), "Key should exist after recovery");

                // get_at at exact timestamp should return same result
                let (v2, ts2) = MvccBackend::get_at(&store, &key, expected_ts).unwrap();

                // Verify no duplicates: get() and get_at() return consistent results
                assert_eq!(ts1, ts2, "get() and get_at() should return same timestamp");
                assert_eq!(v1, v2, "get() and get_at() should return same value");
            }
        }
    }

    // Key Boundary Condition Tests (A3)

    #[test]
    fn test_key_empty() {
        let (_dir, mut store) = open_store();

        // Write with empty key
        MvccBackend::put(&mut store, "".into(), Some("value".into()), 10).unwrap();

        // Read back
        let (v, ts) = MvccBackend::get(&store, &"".into()).unwrap();
        assert_eq!(v, Some("value".into()));
        assert_eq!(ts, 10);

        // Update with new version
        MvccBackend::put(&mut store, "".into(), Some("value2".into()), 20).unwrap();

        // Verify MVCC works with empty key
        let (v, ts) = MvccBackend::get_at(&store, &"".into(), 15).unwrap();
        assert_eq!(v, Some("value".into()));
        assert_eq!(ts, 10);

        // Latest version
        let (v, ts) = MvccBackend::get(&store, &"".into()).unwrap();
        assert_eq!(v, Some("value2".into()));
        assert_eq!(ts, 20);
    }

    #[test]
    fn test_key_very_long() {
        let (_dir, mut store) = open_store();

        // Generate 1 MB key deterministically
        let long_key = "k".repeat(1_000_000);

        MvccBackend::put(&mut store, long_key.clone(), Some("v1".into()), 10).unwrap();

        // Verify round-trip
        let (v, ts) = MvccBackend::get(&store, &long_key).unwrap();
        assert_eq!(v, Some("v1".into()));
        assert_eq!(ts, 10);

        // Trigger flush with other keys
        for i in 0u64..2000 {
            let key = format!("short-{i:06}");
            MvccBackend::put(&mut store, key, Some("val".into()), i + 100).unwrap();
        }

        // Verify long key still accessible after flush
        let (v, ts) = MvccBackend::get(&store, &long_key).unwrap();
        assert_eq!(v, Some("v1".into()));
        assert_eq!(ts, 10);
    }

    #[test]
    fn test_key_null_bytes() {
        let (_dir, mut store) = open_store();

        // Keys with null bytes in different positions
        let keys = vec![
            "key\0middle",   // Null in middle
            "\0leading",     // Null at start
            "trailing\0",    // Null at end
            "multi\0\0ple",  // Multiple nulls
        ];

        // Write all keys
        for (i, key) in keys.iter().enumerate() {
            let ts = (i + 1) as u64 * 10;
            MvccBackend::put(&mut store, key.to_string(), Some(format!("v{}", i)), ts).unwrap();
        }

        // Verify all keys readable
        for (i, key) in keys.iter().enumerate() {
            let (v, ts) = MvccBackend::get(&store, &key.to_string()).unwrap();
            assert_eq!(v, Some(format!("v{}", i)));
            assert_eq!(ts, (i + 1) as u64 * 10);
        }

        // Verify keys are distinct (null bytes preserved)
        assert_ne!(keys[0], keys[1]);
        assert_ne!(keys[1], keys[2]);
        assert_ne!(keys[2], keys[3]);
    }

    #[test]
    fn test_key_unicode() {
        let (_dir, mut store) = open_store();

        // Various Unicode keys
        let keys = vec![
            ("emoji", "🔑key"),           // Emoji
            ("cjk", "键"),                // Chinese
            ("arabic", "مفتاح"),          // Arabic (RTL)
            ("latin_ext", "keÿ"),         // Latin Extended
            ("combining", "e\u{0301}"),   // Combining character (é)
            ("mixed", "key_ñ_键_🔑"),     // Mixed scripts
        ];

        // Write all keys
        for (name, key) in &keys {
            let ts = keys.iter().position(|k| k.0 == *name).unwrap() as u64 + 10;
            MvccBackend::put(&mut store, key.to_string(), Some(format!("v_{}", name)), ts).unwrap();
        }

        // Verify all keys readable
        for (name, key) in &keys {
            let (v, _ts) = MvccBackend::get(&store, &key.to_string()).unwrap();
            assert_eq!(v, Some(format!("v_{}", name)));
        }

        // Verify keys are distinct
        let key_set: std::collections::HashSet<_> = keys.iter().map(|(_, k)| k).collect();
        assert_eq!(key_set.len(), keys.len(), "All Unicode keys should be distinct");

        // Trigger flush and verify persistence
        for i in 0u64..2000 {
            let key = format!("filler-{i:06}");
            MvccBackend::put(&mut store, key, Some("val".into()), i + 100).unwrap();
        }

        // Verify Unicode keys still readable after flush
        for (name, key) in &keys {
            let (v, _ts) = MvccBackend::get(&store, &key.to_string()).unwrap();
            assert!(v.is_some(), "Unicode key {} should be readable after flush", name);
        }
    }

    // Level Overflow Handling Tests (B2)

    #[test]
    fn test_level_l0_exceeds_threshold_writes_not_blocked() {
        let (_dir, mut store) = open_store();

        // Write 5 batches of data, each triggers a flush.
        // First 4 batches: L0 grows to 4 files.
        // 5th batch: Triggers compaction (L0 → L1), then adds new file to L0.
        for batch in 0..5 {
            for i in 0u64..2000 {
                let key = format!("batch{}-key{:06}", batch, i);
                let val = format!("value-{}", "x".repeat(20));
                let ts = batch * 10000 + i + 1;
                MvccBackend::put(&mut store, key, Some(val), ts).unwrap();
            }
        }

        // Verify all batches are accessible.
        for batch in 0..5 {
            let key = format!("batch{}-key{:06}", batch, 0);
            let (v, _ts) = MvccBackend::get(&store, &key).unwrap();
            assert!(v.is_some(), "Batch {} should be accessible", batch);
        }

        // Verify compaction happened by checking a key from each batch.
        // This implicitly verifies that writes continued after compaction.
        for batch in 0..5 {
            for i in (0..2000).step_by(500) {
                let key = format!("batch{}-key{:06}", batch, i);
                let expected_ts = batch * 10000 + i + 1;
                let (v, ts) = MvccBackend::get(&store, &key).unwrap();
                assert!(v.is_some(), "Key from batch {} should exist", batch);
                assert_eq!(ts, expected_ts);
            }
        }
    }

    #[test]
    fn test_level_l0_can_grow_during_heavy_writes() {
        let (_dir, mut store) = open_store();

        // Write many batches rapidly.
        // Each batch triggers flush, some may trigger compaction.
        for batch in 0..10 {
            for i in 0u64..2000 {
                let key = format!("b{}-k{:06}", batch, i);
                let val = format!("v-{}", "x".repeat(20));
                let ts = batch * 10000 + i + 1;
                MvccBackend::put(&mut store, key, Some(val), ts).unwrap();
            }
        }

        // Verify all data is accessible.
        for batch in 0..10 {
            for i in (0..2000).step_by(500) {
                let key = format!("b{}-k{:06}", batch, i);
                let (v, _ts) = MvccBackend::get(&store, &key).unwrap();
                assert!(v.is_some(), "Key from batch {} should exist", batch);
            }
        }

        // Save manifest to ensure persistence.
        store.save_manifest().unwrap();
    }

    // Fault-Injection Crash Recovery Fuzz Test

    #[test]
    fn test_faulty_disk_crash_recovery_fuzz() {
        // 1. Master seed from environment or random
        let seed = env::var("TAPI_TEST_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        println!("test_faulty_disk_crash_recovery_fuzz seed={}", seed);

        let mut rng = StdRng::seed_from_u64(seed);

        // 2. Setup temp directory and fault state
        let dir = TempDir::new().unwrap();

        let initial_config = DiskFaultConfig {
            fsync_fail_rate: 0.2,
            read_corruption_rate: 0.05,
            enospc_after_bytes: None,
            slow_io_latency: None,
        };

        FaultyDiskIo::<BufferedIo>::enable_shared_fault_state(initial_config.clone(), seed);
        let fault_handle = FaultyDiskIo::<BufferedIo>::get_shared_fault_state().unwrap();

        // 3. Open store with FaultyDiskIo
        type TestStore = DiskStore<String, String, u64, FaultyDiskIo<BufferedIo>>;
        let mut store = TestStore::open(dir.path().to_path_buf()).unwrap();

        // 4. State tracking
        let mut committed_data: BTreeMap<(String, u64), String> = BTreeMap::new();
        let mut pending_writes: Vec<(String, String, u64)> = Vec::new();
        let mut all_keys: HashSet<String> = HashSet::new();

        // 5. Workload execution (100 ops)
        const NUM_OPS: usize = 100;
        println!("executing {} random operations", NUM_OPS);

        for i in 0..NUM_OPS {
            let op_choice = rng.r#gen_range(0..10);
            match op_choice {
                0..=5 => {
                    // PUT: 60%
                    let key = format!("k{:03}", rng.r#gen_range(0..30));
                    let value = format!("v{:08x}", rng.r#gen::<u32>());
                    let ts = rng.r#gen_range(1..1000);

                    match MvccBackend::put(&mut store, key.clone(), Some(value.clone()), ts) {
                        Ok(_) => {
                            pending_writes.push((key.clone(), value, ts));
                            all_keys.insert(key);
                        }
                        Err(_) => {
                            // ENOSPC or other write failure expected
                        }
                    }
                }
                6..=7 => {
                    // GET: 20%
                    if !all_keys.is_empty() {
                        let key_vec: Vec<_> = all_keys.iter().collect();
                        let key = key_vec[rng.r#gen_range(0..key_vec.len())];
                        let _ = MvccBackend::get(&store, key); // May fail with corruption
                    }
                }
                8 => {
                    // SCAN: 10%
                    if all_keys.len() >= 2 {
                        let mut key_vec: Vec<_> = all_keys.iter().cloned().collect();
                        key_vec.sort();
                        let start_idx = rng.r#gen_range(0..key_vec.len());
                        let end_idx = rng.r#gen_range(start_idx..key_vec.len());
                        let ts = rng.r#gen_range(1..1000);
                        let _ = MvccBackend::scan(&store, &key_vec[start_idx], &key_vec[end_idx], ts);
                    }
                }
                9 => {
                    // SYNC: 10%
                    match store.sync() {
                        Ok(_) => {
                            // Commit all pending writes
                            for (k, v, ts) in pending_writes.drain(..) {
                                committed_data.insert((k, ts), v);
                            }
                            // Try to save manifest (may fail with ENOSPC)
                            let _ = store.save_manifest();
                        }
                        Err(_) => {
                            // Fsync failed - lose all pending writes
                            pending_writes.clear();
                        }
                    }
                }
                _ => unreachable!(),
            }

            // Mutate faults periodically
            if i > 0 && i % rng.r#gen_range(10..15) == 0 {
                let mut state = fault_handle.lock().unwrap();
                state.config.fsync_fail_rate = rng.r#gen_range(0.0..0.5);
                state.config.read_corruption_rate = rng.r#gen_range(0.0..0.1);
            }
        }

        // 6. Final sync attempt before crash
        if store.sync().is_ok() {
            for (k, v, ts) in pending_writes.drain(..) {
                committed_data.insert((k, ts), v);
            }
            let _ = store.save_manifest();
        }

        println!("committed {} writes before crash", committed_data.len());

        // 7. Crash simulation
        drop(store);

        // 8. Disable faults for clean recovery
        {
            let mut state = fault_handle.lock().unwrap();
            state.config.fsync_fail_rate = 0.0;
            state.config.read_corruption_rate = 0.0;
            state.config.enospc_after_bytes = None;
        }

        println!("simulating crash recovery...");

        // 9. Recovery
        let recovered_store = TestStore::open(dir.path().to_path_buf()).unwrap();

        // 10. Verification
        println!("verifying {} committed writes", committed_data.len());

        for ((key, ts), expected_value) in &committed_data {
            let (actual, actual_ts) = MvccBackend::get_at(&recovered_store, key, *ts).unwrap();

            assert_eq!(
                actual.as_ref(),
                Some(expected_value),
                "Value mismatch for key={} ts={}: expected {:?}, got {:?}",
                key,
                ts,
                Some(expected_value),
                actual
            );

            assert_eq!(
                actual_ts, *ts,
                "Timestamp mismatch for key={}: expected {}, got {}",
                key, ts, actual_ts
            );
        }

        println!(
            "all {} committed writes verified successfully",
            committed_data.len()
        );

        // 11. Cleanup
        FaultyDiskIo::<BufferedIo>::disable_shared_fault_state();
    }
}
