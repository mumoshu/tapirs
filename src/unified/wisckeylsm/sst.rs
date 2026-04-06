use crate::mvcc::disk::aligned_buf::{AlignedBuf, BLOCK_SIZE, round_up};
use crate::mvcc::disk::disk_io::{DiskIo, OpenFlags};
use crate::mvcc::disk::error::StorageError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// SST metadata stored in manifests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstMeta {
    pub id: u64,
    pub path: PathBuf,
    pub num_entries: u64,
}

/// On-disk footer (last block of the SSTable file).
#[derive(Debug, Serialize, Deserialize)]
struct Footer {
    index_offset: u64,
    index_length: u32,
    bloom_offset: u64,
    bloom_length: u32,
    num_entries: u64,
    footer_crc: u32,
}

/// Index entry: first key of a data block + its file offset.
#[derive(Debug, Serialize, Deserialize)]
struct IndexEntry<K> {
    first_key: K,
    offset: u64,
    length: u32,
    block_crc: u32,
}

/// Simple bloom filter (bit array + k hash functions via double hashing).
#[derive(Debug, Serialize, Deserialize)]
struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
}

impl BloomFilter {
    fn new(expected_items: usize) -> Self {
        let num_bits = (expected_items * 10).max(64);
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_hashes: 7,
        }
    }

    fn insert(&mut self, data: &[u8]) {
        let (h1, h2) = Self::hash_pair(data);
        let num_bits = self.bits.len() as u64 * 64;
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % num_bits;
            self.bits[(bit / 64) as usize] |= 1 << (bit % 64);
        }
    }

    fn may_contain(&self, data: &[u8]) -> bool {
        let (h1, h2) = Self::hash_pair(data);
        let num_bits = self.bits.len() as u64 * 64;
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % num_bits;
            if self.bits[(bit / 64) as usize] & (1 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash_pair(data: &[u8]) -> (u64, u64) {
        use std::hash::{Hash, Hasher};

        let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
        data.hash(&mut hasher1);
        let h1 = hasher1.finish();

        let mut hasher2 = std::collections::hash_map::DefaultHasher::new();
        0xDEAD_BEEFu64.hash(&mut hasher2);
        data.hash(&mut hasher2);
        let h2 = hasher2.finish() | 1;

        (h1, h2)
    }
}

/// Writes sorted (K, V) pairs to a generic SSTable file.
pub(crate) struct SSTableWriter;

impl SSTableWriter {
    pub async fn write<K, V, IO>(
        path: &Path,
        entries: &BTreeMap<K, V>,
        io_flags: OpenFlags,
    ) -> Result<u64, StorageError>
    where
        K: Serialize + Clone,
        V: Serialize + Clone,
        IO: DiskIo,
    {
        let io = IO::open(path, io_flags, crate::mvcc::disk::disk_io::OpenMode::OpenImmutable)?;
        let mut offset: u64 = 0;
        let mut index_entries: Vec<IndexEntry<K>> = Vec::new();
        let mut bloom = BloomFilter::new(entries.len());

        let mut block_buf: Vec<u8> = Vec::new();
        let mut block_first_key: Option<K> = None;
        let mut count: u64 = 0;

        for (key, value) in entries {
            let key_bytes =
                bitcode::serialize(key).map_err(|e| StorageError::Codec(e.to_string()))?;
            bloom.insert(&key_bytes);

            let pair = (key, value);
            let serialized =
                bitcode::serialize(&pair).map_err(|e| StorageError::Codec(e.to_string()))?;

            if !block_buf.is_empty() && block_buf.len() + serialized.len() + 4 > BLOCK_SIZE - 8 {
                let idx =
                    Self::flush_block::<K, IO>(&io, &block_buf, &mut offset, &block_first_key)
                        .await?;
                index_entries.push(idx);
                block_buf.clear();
                block_first_key = None;
            }

            if block_first_key.is_none() {
                block_first_key = Some(key.clone());
            }

            block_buf.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            block_buf.extend_from_slice(&serialized);
            count += 1;
        }

        if !block_buf.is_empty() {
            let idx =
                Self::flush_block::<K, IO>(&io, &block_buf, &mut offset, &block_first_key).await?;
            index_entries.push(idx);
        }

        // Write index block.
        let index_bytes =
            bitcode::serialize(&index_entries).map_err(|e| StorageError::Codec(e.to_string()))?;
        let index_offset = offset;
        let index_padded = round_up(index_bytes.len());
        let mut index_buf = AlignedBuf::new(index_padded);
        index_buf.fill(&index_bytes);
        io.pwrite(&index_buf, offset).await?;
        offset += index_padded as u64;

        // Write bloom filter block.
        let bloom_bytes =
            bitcode::serialize(&bloom).map_err(|e| StorageError::Codec(e.to_string()))?;
        let bloom_offset = offset;
        let bloom_padded = round_up(bloom_bytes.len());
        let mut bloom_buf = AlignedBuf::new(bloom_padded);
        bloom_buf.fill(&bloom_bytes);
        io.pwrite(&bloom_buf, offset).await?;
        offset += bloom_padded as u64;

        // Write footer block.
        let footer = Footer {
            index_offset,
            index_length: index_bytes.len() as u32,
            bloom_offset,
            bloom_length: bloom_bytes.len() as u32,
            num_entries: count,
            footer_crc: 0,
        };
        let footer_bytes =
            bitcode::serialize(&footer).map_err(|e| StorageError::Codec(e.to_string()))?;
        let footer_len = footer_bytes.len() as u32;
        let mut footer_buf = AlignedBuf::new(BLOCK_SIZE);
        let slice = footer_buf.as_full_slice_mut();
        slice[..footer_bytes.len()].copy_from_slice(&footer_bytes);
        let len_offset = BLOCK_SIZE - 4;
        slice[len_offset..len_offset + 4].copy_from_slice(&footer_len.to_le_bytes());
        footer_buf.set_len(BLOCK_SIZE);
        io.pwrite(&footer_buf, offset).await?;
        io.fsync().await?;

        io.close();
        Ok(count)
    }

    async fn flush_block<K, IO>(
        io: &IO,
        block_data: &[u8],
        offset: &mut u64,
        first_key: &Option<K>,
    ) -> Result<IndexEntry<K>, StorageError>
    where
        K: Clone,
        IO: DiskIo,
    {
        let block_crc = crc32fast::hash(block_data);
        let total = block_data.len() + 4;
        let padded = round_up(total);
        let mut buf = AlignedBuf::new(padded);
        let slice = buf.as_full_slice_mut();
        slice[..block_data.len()].copy_from_slice(block_data);
        slice[block_data.len()..block_data.len() + 4]
            .copy_from_slice(&block_crc.to_le_bytes());
        buf.set_len(total);

        let block_offset = *offset;
        io.pwrite(&buf, block_offset).await?;
        *offset += padded as u64;

        Ok(IndexEntry {
            first_key: first_key.clone().expect("block must have a first key"),
            offset: block_offset,
            length: total as u32,
            block_crc,
        })
    }
}

/// Reads entries from a generic SSTable file.
pub(crate) struct SSTableReader<IO: DiskIo> {
    io: IO,
    path: PathBuf,
    footer: Footer,
}

impl<IO: DiskIo> SSTableReader<IO> {
    pub async fn open(path: PathBuf, flags: OpenFlags) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags, crate::mvcc::disk::disk_io::OpenMode::OpenImmutable)?;

        let file_size = io.file_len()?;
        if file_size < BLOCK_SIZE as u64 {
            return Err(StorageError::Corruption {
                file: path.display().to_string(),
                offset: 0,
                expected_crc: 0,
                actual_crc: 0,
            });
        }
        let footer_offset = file_size - BLOCK_SIZE as u64;
        let mut footer_buf = AlignedBuf::new(BLOCK_SIZE);
        io.pread(&mut footer_buf, footer_offset).await?;

        let raw = footer_buf.as_full_slice();
        let len_offset = BLOCK_SIZE - 4;
        let footer_len = u32::from_le_bytes([
            raw[len_offset],
            raw[len_offset + 1],
            raw[len_offset + 2],
            raw[len_offset + 3],
        ]) as usize;

        let footer: Footer = bitcode::deserialize(&raw[..footer_len])
            .map_err(|e| StorageError::Codec(e.to_string()))?;

        Ok(Self { io, path, footer })
    }

    pub async fn may_contain_key<K: Serialize>(&self, key: &K) -> Result<bool, StorageError> {
        let bloom_size = round_up(self.footer.bloom_length as usize);
        let mut buf = AlignedBuf::new(bloom_size);
        self.io.pread(&mut buf, self.footer.bloom_offset).await?;

        let bloom: BloomFilter =
            bitcode::deserialize(&buf.as_full_slice()[..self.footer.bloom_length as usize])
                .map_err(|e| StorageError::Codec(e.to_string()))?;

        let key_bytes =
            bitcode::serialize(key).map_err(|e| StorageError::Codec(e.to_string()))?;
        Ok(bloom.may_contain(&key_bytes))
    }

    async fn read_index<K>(&self) -> Result<Vec<IndexEntry<K>>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
    {
        let index_size = round_up(self.footer.index_length as usize);
        let mut buf = AlignedBuf::new(index_size);
        self.io.pread(&mut buf, self.footer.index_offset).await?;

        let index: Vec<IndexEntry<K>> =
            bitcode::deserialize(&buf.as_full_slice()[..self.footer.index_length as usize])
                .map_err(|e| StorageError::Codec(e.to_string()))?;
        Ok(index)
    }

    async fn read_block<K, V>(
        &self,
        idx: &IndexEntry<K>,
    ) -> Result<Vec<(K, V)>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
    {
        let padded = round_up(idx.length as usize);
        let mut buf = AlignedBuf::new(padded);
        self.io.pread(&mut buf, idx.offset).await?;

        let raw = buf.as_full_slice();
        let data_len = idx.length as usize - 4;
        let data = &raw[..data_len];
        let stored_crc = u32::from_le_bytes([
            raw[data_len],
            raw[data_len + 1],
            raw[data_len + 2],
            raw[data_len + 3],
        ]);

        if stored_crc != idx.block_crc {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: idx.offset,
                expected_crc: idx.block_crc,
                actual_crc: stored_crc,
            });
        }

        let actual_crc = crc32fast::hash(data);
        if actual_crc != idx.block_crc {
            return Err(StorageError::Corruption {
                file: self.path.display().to_string(),
                offset: idx.offset,
                expected_crc: idx.block_crc,
                actual_crc,
            });
        }

        let mut entries = Vec::new();
        let mut pos = 0;
        while pos + 4 <= data_len {
            let entry_len =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;
            if pos + entry_len > data_len {
                break;
            }
            let (k, v): (K, V) = bitcode::deserialize(&data[pos..pos + entry_len])
                .map_err(|e| StorageError::Codec(e.to_string()))?;
            entries.push((k, v));
            pos += entry_len;
        }

        Ok(entries)
    }

    pub async fn get<K, V>(&self, target: &K) -> Result<Option<V>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
        V: for<'de> Deserialize<'de>,
    {
        let index = self.read_index::<K>().await?;
        if index.is_empty() {
            return Ok(None);
        }

        let block_idx = match index.binary_search_by(|entry| entry.first_key.cmp(target)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        let entries = self.read_block::<K, V>(&index[block_idx]).await?;
        for (k, v) in entries {
            if k == *target {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Return the first (K, V) entry with key >= `from`, or None.
    /// Uses binary search on the block index, then scans forward.
    pub async fn range_get_first<K, V>(&self, from: &K) -> Result<Option<(K, V)>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
        V: for<'de> Deserialize<'de>,
    {
        let index = self.read_index::<K>().await?;
        if index.is_empty() {
            return Ok(None);
        }

        // Find the block whose first_key <= from (the key could be in that block).
        let block_idx = match index.binary_search_by(|entry| entry.first_key.cmp(from)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        // Scan from candidate block forward until we find key >= from.
        for idx_entry in index.iter().skip(block_idx) {
            let entries = self.read_block::<K, V>(idx_entry).await?;
            for (k, v) in entries {
                if k >= *from {
                    return Ok(Some((k, v)));
                }
            }
        }
        Ok(None)
    }

    pub async fn read_all<K, V>(&self) -> Result<Vec<(K, V)>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
    {
        let index = self.read_index::<K>().await?;
        let mut all = Vec::new();
        for idx_entry in &index {
            let entries = self.read_block::<K, V>(idx_entry).await?;
            all.extend(entries);
        }
        Ok(all)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::disk::disk_io::BufferedIo;
    use tempfile::NamedTempFile;

    fn test_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            direct: false,
        }
    }

    #[tokio::test]
    async fn write_and_read_all_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut entries = BTreeMap::new();
        for i in 0u64..10 {
            entries.insert(format!("key-{i:03}"), format!("val-{i:03}"));
        }

        let count = SSTableWriter::write::<String, String, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();
        assert_eq!(count, 10);

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();
        let all = reader.read_all::<String, String>().await.unwrap();
        assert_eq!(all.len(), 10);
        assert_eq!(all[0], ("key-000".to_string(), "val-000".to_string()));
        assert_eq!(all[9], ("key-009".to_string(), "val-009".to_string()));
    }

    #[tokio::test]
    async fn get_exact_key_lookup() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut entries = BTreeMap::new();
        for i in 0u64..10 {
            entries.insert(format!("key-{i:03}"), i * 100);
        }

        SSTableWriter::write::<String, u64, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        let result = reader
            .get::<String, u64>(&"key-005".to_string())
            .await
            .unwrap();
        assert_eq!(result, Some(500));

        let result = reader
            .get::<String, u64>(&"key-009".to_string())
            .await
            .unwrap();
        assert_eq!(result, Some(900));

        let result = reader
            .get::<String, u64>(&"missing".to_string())
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn bloom_filter_check() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut entries = BTreeMap::new();
        for i in 0u64..20 {
            entries.insert(format!("bloom-{i:03}"), i);
        }

        SSTableWriter::write::<String, u64, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // Present keys should pass the bloom filter.
        for i in 0u64..20 {
            let key = format!("bloom-{i:03}");
            assert!(reader.may_contain_key(&key).await.unwrap());
        }

        // Absent keys should mostly fail (bloom has ~1% FP rate).
        let mut false_positives = 0;
        for i in 100u64..200 {
            let key = format!("absent-{i:03}");
            if reader.may_contain_key(&key).await.unwrap() {
                false_positives += 1;
            }
        }
        assert!(false_positives < 10, "too many false positives: {false_positives}");
    }

    #[tokio::test]
    async fn empty_sst() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let entries: BTreeMap<String, String> = BTreeMap::new();

        let count =
            SSTableWriter::write::<String, String, BufferedIo>(&path, &entries, test_flags())
                .await
                .unwrap();
        assert_eq!(count, 0);

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();
        let result = reader
            .get::<String, String>(&"any".to_string())
            .await
            .unwrap();
        assert_eq!(result, None);

        let all = reader.read_all::<String, String>().await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn multiple_data_blocks() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Create enough entries to span multiple 4KiB blocks.
        // Each entry is ~200 bytes serialized, so ~20 entries per block.
        let mut entries = BTreeMap::new();
        for i in 0u64..100 {
            let key = format!("multiblock-key-{i:04}");
            let value = format!("value-{}", "x".repeat(150));
            entries.insert(key, value);
        }

        let count =
            SSTableWriter::write::<String, String, BufferedIo>(&path, &entries, test_flags())
                .await
                .unwrap();
        assert_eq!(count, 100);

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // Verify we can read entries from different blocks.
        let first = reader
            .get::<String, String>(&"multiblock-key-0000".to_string())
            .await
            .unwrap();
        assert!(first.is_some());

        let last = reader
            .get::<String, String>(&"multiblock-key-0099".to_string())
            .await
            .unwrap();
        assert!(last.is_some());

        let middle = reader
            .get::<String, String>(&"multiblock-key-0050".to_string())
            .await
            .unwrap();
        assert!(middle.is_some());

        let all = reader.read_all::<String, String>().await.unwrap();
        assert_eq!(all.len(), 100);
    }

    async fn build_sst_for_range_tests() -> (PathBuf, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut entries = BTreeMap::new();
        for i in (0u64..10).map(|i| i * 10) {
            entries.insert(format!("key-{i:03}"), format!("val-{i:03}"));
        }
        // keys: key-000, key-010, key-020, ..., key-090
        SSTableWriter::write::<String, String, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();
        (path, tmp)
    }

    #[tokio::test]
    async fn range_get_first_exact_match() {
        let (path, _tmp) = build_sst_for_range_tests().await;
        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        let result = reader
            .range_get_first::<String, String>(&"key-030".to_string())
            .await
            .unwrap();
        assert_eq!(result, Some(("key-030".to_string(), "val-030".to_string())));
    }

    #[tokio::test]
    async fn range_get_first_between_keys() {
        let (path, _tmp) = build_sst_for_range_tests().await;
        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // key-025 doesn't exist; should return key-030 (next >=).
        let result = reader
            .range_get_first::<String, String>(&"key-025".to_string())
            .await
            .unwrap();
        assert_eq!(result, Some(("key-030".to_string(), "val-030".to_string())));
    }

    #[tokio::test]
    async fn range_get_first_before_all() {
        let (path, _tmp) = build_sst_for_range_tests().await;
        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // Before all keys — should return the first entry.
        let result = reader
            .range_get_first::<String, String>(&"aaa".to_string())
            .await
            .unwrap();
        assert_eq!(result, Some(("key-000".to_string(), "val-000".to_string())));
    }

    #[tokio::test]
    async fn range_get_first_after_all() {
        let (path, _tmp) = build_sst_for_range_tests().await;
        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // After all keys — should return None.
        let result = reader
            .range_get_first::<String, String>(&"zzz".to_string())
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn range_get_first_multi_block() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Large entries to span multiple 4KiB blocks.
        let mut entries = BTreeMap::new();
        for i in 0u64..100 {
            let key = format!("mb-{i:04}");
            let value = format!("val-{}", "x".repeat(150));
            entries.insert(key, value);
        }
        SSTableWriter::write::<String, String, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // First key.
        let result = reader
            .range_get_first::<String, String>(&"mb-0000".to_string())
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, "mb-0000");

        // Key between blocks.
        let result = reader
            .range_get_first::<String, String>(&"mb-0055".to_string())
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, "mb-0055");

        // Key not present, should find next.
        let result = reader
            .range_get_first::<String, String>(&"mb-0050a".to_string())
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, "mb-0051");

        // Last key.
        let result = reader
            .range_get_first::<String, String>(&"mb-0099".to_string())
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, "mb-0099");
    }
}
