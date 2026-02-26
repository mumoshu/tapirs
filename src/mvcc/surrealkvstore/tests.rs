use super::*;
use crate::mvcc::backend::MvccBackend;
use tempfile::TempDir;

type TestStore = SurrealKvStore<String, String, u64>;

fn open_store() -> (TempDir, TestStore) {
    let dir = TempDir::new().unwrap();
    let store = TestStore::open(dir.path().to_path_buf()).unwrap();
    (dir, store)
}

#[test]
fn get_nonexistent_key() {
    let (_dir, store) = open_store();
    let (val, ts) = store.get(&"missing".to_string()).unwrap();
    assert_eq!(val, None);
    assert_eq!(ts, 0);
}

#[test]
fn put_and_get() {
    let (_dir, mut store) = open_store();
    store
        .put("key1".into(), Some("value1".into()), 10)
        .unwrap();
    let (val, ts) = store.get(&"key1".into()).unwrap();
    assert_eq!(val, Some("value1".into()));
    assert_eq!(ts, 10);
}

#[test]
fn multiple_keys() {
    let (_dir, mut store) = open_store();
    store.put("a".into(), Some("va".into()), 1).unwrap();
    store.put("b".into(), Some("vb".into()), 2).unwrap();
    store.put("c".into(), Some("vc".into()), 3).unwrap();

    let (val, ts) = store.get(&"a".into()).unwrap();
    assert_eq!(val, Some("va".into()));
    assert_eq!(ts, 1);

    let (val, ts) = store.get(&"b".into()).unwrap();
    assert_eq!(val, Some("vb".into()));
    assert_eq!(ts, 2);

    let (val, ts) = store.get(&"c".into()).unwrap();
    assert_eq!(val, Some("vc".into()));
    assert_eq!(ts, 3);
}

#[test]
fn get_at_returns_correct_version() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v10".into()), 10).unwrap();
    store.put("k".into(), Some("v20".into()), 20).unwrap();

    // Exact timestamps
    let (val, ts) = store.get_at(&"k".into(), 10).unwrap();
    assert_eq!(val, Some("v10".into()));
    assert_eq!(ts, 10);

    let (val, ts) = store.get_at(&"k".into(), 20).unwrap();
    assert_eq!(val, Some("v20".into()));
    assert_eq!(ts, 20);

    // Between versions: ts=15 should get version at ts=10
    let (val, ts) = store.get_at(&"k".into(), 15).unwrap();
    assert_eq!(val, Some("v10".into()));
    assert_eq!(ts, 10);

    // Before all versions
    let (val, ts) = store.get_at(&"k".into(), 5).unwrap();
    assert_eq!(val, None);
    assert_eq!(ts, 0);

    // After all versions: should get latest
    let (val, ts) = store.get_at(&"k".into(), 25).unwrap();
    assert_eq!(val, Some("v20".into()));
    assert_eq!(ts, 20);
}

#[test]
fn tombstone_deletes_key() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("alive".into()), 10).unwrap();
    store.put("k".into(), None, 20).unwrap();

    // Latest version is tombstone
    let (val, ts) = store.get(&"k".into()).unwrap();
    assert_eq!(val, None);
    assert_eq!(ts, 20);

    // Old version still accessible via get_at
    let (val, ts) = store.get_at(&"k".into(), 10).unwrap();
    assert_eq!(val, Some("alive".into()));
    assert_eq!(ts, 10);
}

#[test]
fn get_range_single_version() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v".into()), 10).unwrap();

    let (at_ts, next_ts) = store.get_range(&"k".into(), 10).unwrap();
    assert_eq!(at_ts, 10);
    assert_eq!(next_ts, None);
}

#[test]
fn get_range_multiple_versions() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v10".into()), 10).unwrap();
    store.put("k".into(), Some("v20".into()), 20).unwrap();

    // At ts=15, version is at 10, next is at 20
    let (at_ts, next_ts) = store.get_range(&"k".into(), 15).unwrap();
    assert_eq!(at_ts, 10);
    assert_eq!(next_ts, Some(20));
}

#[test]
fn scan_basic() {
    let (_dir, mut store) = open_store();
    store.put("a".into(), Some("va".into()), 10).unwrap();
    store.put("b".into(), Some("vb".into()), 10).unwrap();
    store.put("c".into(), Some("vc".into()), 10).unwrap();
    store.put("d".into(), Some("vd".into()), 10).unwrap();

    let results = store.scan(&"b".into(), &"c".into(), 10).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, "b");
    assert_eq!(results[0].1, Some("vb".into()));
    assert_eq!(results[1].0, "c");
    assert_eq!(results[1].1, Some("vc".into()));
}

#[test]
fn scan_with_tombstone() {
    let (_dir, mut store) = open_store();
    store.put("a".into(), Some("va".into()), 10).unwrap();
    store.put("b".into(), None, 10).unwrap();
    store.put("c".into(), Some("vc".into()), 10).unwrap();

    let results = store.scan(&"a".into(), &"c".into(), 10).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[1].0, "b");
    assert_eq!(results[1].1, None);
}

#[test]
fn has_writes_in_range_true() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v".into()), 10).unwrap();

    assert!(store
        .has_writes_in_range(&"k".into(), &"k".into(), 5, 15)
        .unwrap());
}

#[test]
fn has_writes_in_range_false() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v".into()), 10).unwrap();

    // Timestamp range doesn't include 10 (exclusive bounds)
    assert!(!store
        .has_writes_in_range(&"k".into(), &"k".into(), 10, 15)
        .unwrap());
    assert!(!store
        .has_writes_in_range(&"k".into(), &"k".into(), 5, 10)
        .unwrap());
}

#[test]
fn commit_get_and_last_read() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v".into()), 10).unwrap();

    store.commit_get("k".into(), 10, 50).unwrap();

    let last_read = store.get_last_read(&"k".into()).unwrap();
    assert_eq!(last_read, Some(50));
}

#[test]
fn get_last_read_at() {
    let (_dir, mut store) = open_store();
    store.put("k".into(), Some("v10".into()), 10).unwrap();
    store.put("k".into(), Some("v20".into()), 20).unwrap();

    store.commit_get("k".into(), 10, 50).unwrap();

    // Last read at version 10 should be 50
    let last_read = store.get_last_read_at(&"k".into(), 10).unwrap();
    assert_eq!(last_read, Some(50));

    // Last read at version 20 should be None (not committed)
    let last_read = store.get_last_read_at(&"k".into(), 20).unwrap();
    assert_eq!(last_read, None);
}

#[test]
fn commit_batch_multi_key() {
    let (_dir, mut store) = open_store();
    // Pre-populate keys for read tracking
    store.put("r1".into(), Some("vr1".into()), 5).unwrap();
    store.put("r2".into(), Some("vr2".into()), 5).unwrap();

    let writes = vec![
        ("w1".into(), Some("vw1".into())),
        ("w2".into(), Some("vw2".into())),
    ];
    let reads = vec![("r1".into(), 5_u64), ("r2".into(), 5_u64)];

    store.commit_batch(writes, reads, 10).unwrap();

    // Check writes applied
    let (val, ts) = store.get(&"w1".into()).unwrap();
    assert_eq!(val, Some("vw1".into()));
    assert_eq!(ts, 10);

    let (val, ts) = store.get(&"w2".into()).unwrap();
    assert_eq!(val, Some("vw2".into()));
    assert_eq!(ts, 10);

    // Check read timestamps updated
    let last_read = store.get_last_read(&"r1".into()).unwrap();
    assert_eq!(last_read, Some(10));
}

#[test]
fn serialize_deserialize_roundtrip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Write data and serialize, then drop the store to release the lock.
    let serialized = {
        let mut store = TestStore::open(path.clone()).unwrap();
        store.put("k".into(), Some("v".into()), 10).unwrap();
        store.commit_get("k".into(), 10, 50).unwrap();

        // Verify data is readable before serialization.
        let (val, ts) = store.get(&"k".into()).unwrap();
        assert_eq!(val, Some("v".into()), "pre-serialize get failed");
        assert_eq!(ts, 10);

        bitcode::serialize(&store).unwrap()
    };

    // Deserialize reopens the store from the saved path.
    let store2: TestStore = bitcode::deserialize(&serialized).unwrap();

    // Data preserved
    let (val, ts) = store2.get(&"k".into()).unwrap();
    assert_eq!(val, Some("v".into()));
    assert_eq!(ts, 10);

    // Note: last_read_ts is NOT preserved across serialize/deserialize
    // because Memtable is rebuilt from surrealkv (which doesn't store OCC metadata).
    let last_read = store2.get_last_read(&"k".into()).unwrap();
    assert_eq!(last_read, None);
}
