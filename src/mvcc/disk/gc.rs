use super::disk_io::{DiskIo, OpenFlags};
use super::error::StorageError;
use super::lsm::LsmTree;
use super::memtable::{CompositeKey, LsmEntry};
use super::sstable::SSTableReader;
use super::vlog::VlogSegment;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Garbage collection for the vlog and LSM tree.
pub struct GarbageCollector;

impl GarbageCollector {
    /// Scan a vlog segment and rewrite live entries to a new segment.
    ///
    /// An entry is "live" if it still exists in the LSM index (i.e.,
    /// the LSM entry's value_ptr points to this segment and offset).
    ///
    /// Returns stats and a list of `(key, timestamp, new_ptr)` for each
    /// live entry recopied. The caller must update memtable/LSM pointers
    /// from old locations to new ones before deleting the old segment.
    pub async fn gc_vlog_segment<K, V, TS, IO>(
        old_segment: &VlogSegment<IO>,
        new_segment: &mut VlogSegment<IO>,
        lsm: &LsmTree<IO>,
    ) -> Result<(GcStats, Vec<(K, TS, super::vlog::ValuePointer)>), StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone + std::fmt::Debug,
        V: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug,
        TS: Serialize
            + for<'de> Deserialize<'de>
            + Ord
            + Copy
            + Default
            + super::memtable::MaxValue
            + Clone
            + std::fmt::Debug,
        IO: DiskIo,
    {
        let mut stats = GcStats {
            entries_scanned: 0,
            entries_live: 0,
            entries_dead: 0,
            bytes_reclaimed: 0,
        };
        let mut pointer_updates = Vec::new();

        // Scan entire old segment from offset 0 to get entry boundaries.
        // recover_entries() still deserializes all fields — Commit 6 replaces
        // this with recover_pointers() which skips deserialization entirely.
        let entries = old_segment.recover_entries::<K, V, TS>(0).await?;

        for (_entry, old_ptr) in entries {
            stats.entries_scanned += 1;

            // Read only key+ts (skips value bytes entirely). Per WiscKey
            // (Lu et al., FAST'16, Section 3.3.2), the per-field format
            // enables reading just the key portion. Dead entries (the
            // majority during GC) never touch value bytes.
            let (key, ts): (K, TS) = old_segment.read_key_ts(&old_ptr).await?;

            // Check if LSM still references this entry at this location
            match lsm.get_at(&key, &ts).await? {
                Some((_, lsm_entry)) => {
                    // Check if the LSM entry's pointer matches the old segment location
                    if lsm_entry.value_ptr == Some(old_ptr) {
                        // LIVE: Read full entry (including value) for recopy
                        let entry: super::vlog::VlogEntry<K, V, TS> =
                            old_segment.read(&old_ptr).await?;
                        let new_ptr = new_segment.append(&entry).await?;
                        pointer_updates.push((key, ts, new_ptr));
                        stats.entries_live += 1;
                    } else {
                        // DEAD: LSM points to a different location (or None)
                        stats.entries_dead += 1;
                        stats.bytes_reclaimed += old_ptr.length as u64;
                    }
                }
                None => {
                    // DEAD: Entry not found in LSM (deleted or overwritten)
                    stats.entries_dead += 1;
                    stats.bytes_reclaimed += old_ptr.length as u64;
                }
            }
        }

        Ok((stats, pointer_updates))
    }

    /// Prune old versions from the LSM tree during compaction.
    ///
    /// Removes versions with timestamp < `gc_watermark` that are not
    /// the latest version of their key.
    pub fn prune_versions<K, TS>(
        entries: &BTreeMap<CompositeKey<K, TS>, LsmEntry>,
        gc_watermark: TS,
    ) -> BTreeMap<CompositeKey<K, TS>, LsmEntry>
    where
        K: Ord + Clone + Eq,
        TS: Ord + Clone + Copy,
    {
        let mut result = BTreeMap::new();
        let mut last_key: Option<K> = None;

        for (ck, entry) in entries {
            let is_first_version = last_key.as_ref() != Some(&ck.key);

            if is_first_version {
                // Always keep the latest version of each key.
                last_key = Some(ck.key.clone());
                result.insert(ck.clone(), entry.clone());
            } else if ck.timestamp.0 >= gc_watermark {
                // Keep versions at or after the watermark.
                result.insert(ck.clone(), entry.clone());
            }
            // Otherwise: version is below watermark and not the latest — prune it.
        }

        result
    }

    /// Scrub: verify integrity of all SSTable files.
    ///
    /// Returns a list of corrupted files with their error details.
    pub async fn scrub_sstables<K, TS, IO>(
        lsm: &LsmTree<IO>,
        io_flags: OpenFlags,
    ) -> Vec<(PathBuf, StorageError)>
    where
        K: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
        IO: DiskIo,
    {
        let mut corrupted = Vec::new();

        for meta in lsm.l0_metas().iter().chain(lsm.l1_metas().iter()) {
            match SSTableReader::<IO>::open(meta.path.clone(), io_flags).await {
                Ok(reader) => {
                    if let Err(e) = reader.verify::<K, TS>().await {
                        corrupted.push((meta.path.clone(), e));
                    }
                }
                Err(e) => {
                    corrupted.push((meta.path.clone(), e));
                }
            }
        }

        corrupted
    }
}

/// Statistics from a GC run.
#[derive(Debug, Default)]
pub struct GcStats {
    pub entries_scanned: u64,
    pub entries_live: u64,
    pub entries_dead: u64,
    pub bytes_reclaimed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::vlog::ValuePointer;

    #[test]
    fn prune_old_versions() {
        let mut entries = BTreeMap::new();

        // Key "a": versions at ts=30, 20, 10 (sorted by Reverse<TS>).
        entries.insert(
            CompositeKey::new("a".to_string(), 30u64),
            LsmEntry {
                value_ptr: Some(ValuePointer {
                    segment_id: 0,
                    offset: 0,
                    length: 10,
                }),
                last_read_ts: None,
            },
        );
        entries.insert(
            CompositeKey::new("a".to_string(), 20u64),
            LsmEntry {
                value_ptr: Some(ValuePointer {
                    segment_id: 0,
                    offset: 100,
                    length: 10,
                }),
                last_read_ts: None,
            },
        );
        entries.insert(
            CompositeKey::new("a".to_string(), 10u64),
            LsmEntry {
                value_ptr: Some(ValuePointer {
                    segment_id: 0,
                    offset: 200,
                    length: 10,
                }),
                last_read_ts: None,
            },
        );

        // Key "b": single version at ts=15.
        entries.insert(
            CompositeKey::new("b".to_string(), 15u64),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );

        // Prune with watermark=25: should keep ts=30 (latest for "a"),
        // drop ts=20 and ts=10, keep ts=15 (latest for "b").
        let pruned = GarbageCollector::prune_versions(&entries, 25);

        assert_eq!(pruned.len(), 2);
        assert!(pruned.contains_key(&CompositeKey::new("a".to_string(), 30)));
        assert!(pruned.contains_key(&CompositeKey::new("b".to_string(), 15)));
    }

    #[test]
    fn prune_keeps_recent_versions() {
        let mut entries = BTreeMap::new();
        entries.insert(
            CompositeKey::new("a".to_string(), 10u64),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );
        entries.insert(
            CompositeKey::new("a".to_string(), 5u64),
            LsmEntry {
                value_ptr: None,
                last_read_ts: None,
            },
        );

        // Watermark at 3: both versions are above watermark.
        let pruned = GarbageCollector::prune_versions(&entries, 3);
        assert_eq!(pruned.len(), 2);

        // Watermark at 8: version 5 is below but it's not the latest,
        // so it should be pruned.
        let pruned = GarbageCollector::prune_versions(&entries, 8);
        assert_eq!(pruned.len(), 1);
    }
}
