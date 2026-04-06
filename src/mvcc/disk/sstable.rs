use super::aligned_buf::{AlignedBuf, BLOCK_SIZE, round_up};
use super::disk_io::{DiskIo, OpenMode};
use super::error::StorageError;
use super::memtable::{CompositeKey, LsmEntry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
struct IndexEntry<K, TS> {
    first_key: CompositeKey<K, TS>,
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
        // ~10 bits per item, 7 hashes => ~1% FP rate.
        let num_bits = (expected_items * 10).max(64);
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_hashes: 7,
        }
    }

    fn insert(&mut self, data: &[u8]) {
        let (h1, h2) = self.hash_pair(data);
        let num_bits = self.bits.len() as u64 * 64;
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % num_bits;
            self.bits[(bit / 64) as usize] |= 1 << (bit % 64);
        }
    }

    fn may_contain(&self, data: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(data);
        let num_bits = self.bits.len() as u64 * 64;
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % num_bits;
            if self.bits[(bit / 64) as usize] & (1 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash_pair(&self, data: &[u8]) -> (u64, u64) {
        // Use CRC32 as basis for double hashing.
        let crc = crc32fast::hash(data);
        let h1 = crc as u64;
        let h2 = (crc as u64).wrapping_mul(0x9E3779B97F4A7C15) | 1;
        (h1, h2)
    }
}

/// Writes a sorted memtable to an SSTable file.
pub struct SSTableWriter;

impl SSTableWriter {
    /// Write `entries` (already sorted) to an SSTable file.
    /// Returns the number of entries written.
    pub async fn write<K, TS, IO>(
        path: &Path,
        entries: &BTreeMap<CompositeKey<K, TS>, LsmEntry>,
        io_flags: super::disk_io::OpenFlags,
    ) -> Result<u64, StorageError>
    where
        K: Serialize + Clone,
        TS: Serialize + Clone,
        IO: DiskIo,
    {
        let io = IO::open(path, io_flags, OpenMode::Existing)?;
        let mut offset: u64 = 0;
        let mut index_entries: Vec<IndexEntry<K, TS>> = Vec::new();
        let mut bloom = BloomFilter::new(entries.len());

        // Write data blocks. Each block holds entries until it
        // approaches BLOCK_SIZE, then we start a new one.
        let mut block_buf: Vec<u8> = Vec::new();
        let mut block_first_key: Option<CompositeKey<K, TS>> = None;
        let mut count: u64 = 0;

        for (ck, entry) in entries {
            // Add key to bloom filter.
            let key_bytes =
                bitcode::serialize(&ck.key).map_err(|e| StorageError::Codec(e.to_string()))?;
            bloom.insert(&key_bytes);

            // Serialize entry.
            let pair = (ck, entry);
            let serialized =
                bitcode::serialize(&pair).map_err(|e| StorageError::Codec(e.to_string()))?;

            // If adding this entry would exceed the target block size,
            // flush the current block first.
            if !block_buf.is_empty() && block_buf.len() + serialized.len() + 4 > BLOCK_SIZE - 8 {
                let idx = Self::flush_block(&io, &block_buf, &mut offset, &block_first_key).await?;
                index_entries.push(idx);
                block_buf.clear();
                block_first_key = None;
            }

            if block_first_key.is_none() {
                block_first_key = Some(ck.clone());
            }

            // Prefix each entry with its length (4 bytes LE).
            block_buf.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            block_buf.extend_from_slice(&serialized);
            count += 1;
        }

        // Flush remaining block.
        if !block_buf.is_empty() {
            let idx = Self::flush_block(&io, &block_buf, &mut offset, &block_first_key).await?;
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
        // Layout: [footer_bytes | ... padding ... | 4-byte footer_len LE]
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
        // Store footer length in the last 4 bytes of the block.
        let len_offset = BLOCK_SIZE - 4;
        slice[len_offset..len_offset + 4].copy_from_slice(&footer_len.to_le_bytes());
        footer_buf.set_len(BLOCK_SIZE);
        io.pwrite(&footer_buf, offset).await?;
        io.fsync().await?;

        io.close();
        Ok(count)
    }

    async fn flush_block<K, TS, IO>(
        io: &IO,
        block_data: &[u8],
        offset: &mut u64,
        first_key: &Option<CompositeKey<K, TS>>,
    ) -> Result<IndexEntry<K, TS>, StorageError>
    where
        K: Clone,
        TS: Clone,
        IO: DiskIo,
    {
        let block_crc = crc32fast::hash(block_data);
        // Block format: [data | 4-byte CRC32 | padding to 4KiB]
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

/// Reads entries from an SSTable file.
pub struct SSTableReader<IO: DiskIo> {
    io: IO,
    path: PathBuf,
    footer: Footer,
}

impl<IO: DiskIo> SSTableReader<IO> {
    /// Open an SSTable file and read its footer.
    pub async fn open(path: PathBuf, flags: super::disk_io::OpenFlags) -> Result<Self, StorageError> {
        let io = IO::open(&path, flags, OpenMode::Existing)?;

        // Read file size to locate footer (last BLOCK_SIZE bytes).
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

        // Footer length is stored in the last 4 bytes of the block.
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

    /// Check if the bloom filter might contain this key.
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

    /// Read the index entries.
    async fn read_index<K, TS>(&self) -> Result<Vec<IndexEntry<K, TS>>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        let index_size = round_up(self.footer.index_length as usize);
        let mut buf = AlignedBuf::new(index_size);
        self.io.pread(&mut buf, self.footer.index_offset).await?;

        let index: Vec<IndexEntry<K, TS>> =
            bitcode::deserialize(&buf.as_full_slice()[..self.footer.index_length as usize])
                .map_err(|e| StorageError::Codec(e.to_string()))?;
        Ok(index)
    }

    /// Read a single data block and verify its CRC.
    async fn read_block<K, TS>(
        &self,
        idx: &IndexEntry<K, TS>,
    ) -> Result<Vec<(CompositeKey<K, TS>, LsmEntry)>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
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

        // Parse entries from block (length-prefixed).
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
            let (ck, entry): (CompositeKey<K, TS>, LsmEntry) =
                bitcode::deserialize(&data[pos..pos + entry_len])
                    .map_err(|e| StorageError::Codec(e.to_string()))?;
            entries.push((ck, entry));
            pos += entry_len;
        }

        Ok(entries)
    }

    /// Look up a specific composite key.
    pub async fn get<K, TS>(
        &self,
        target: &CompositeKey<K, TS>,
    ) -> Result<Option<LsmEntry>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
        TS: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
    {
        let index = self.read_index::<K, TS>().await?;
        if index.is_empty() {
            return Ok(None);
        }

        // Binary search for the block that might contain the key.
        let block_idx = match index.binary_search_by(|entry| entry.first_key.cmp(target)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        let entries = self.read_block(&index[block_idx]).await?;
        for (ck, entry) in &entries {
            if ck == target {
                return Ok(Some(entry.clone()));
            }
        }
        Ok(None)
    }

    /// Find the latest version of `key` with timestamp <= `ts`.
    pub async fn get_at<K, TS>(
        &self,
        key: &K,
        ts: &TS,
    ) -> Result<Option<(CompositeKey<K, TS>, LsmEntry)>, StorageError>
    where
        K: Serialize + for<'de> Deserialize<'de> + Ord + Clone,
        TS: Serialize + for<'de> Deserialize<'de> + Ord + Clone + super::memtable::MaxValue,
    {
        let index = self.read_index::<K, TS>().await?;
        if index.is_empty() {
            return Ok(None);
        }

        let search = CompositeKey::new(key.clone(), ts.clone());

        // Find the starting block. Because composite keys include
        // Reverse<TS>, a newer timestamp for the same key sorts
        // *before* older ones. We may need to start from block 0
        // even if the search key is before the first index entry.
        let block_idx = match index.binary_search_by(|entry| entry.first_key.cmp(&search)) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        // Scan blocks starting from block_idx.
        for block in index.iter().skip(block_idx) {
            let entries = self.read_block(block).await?;
            for (ck, entry) in entries {
                if ck.key == *key && ck.timestamp.0 <= *ts {
                    return Ok(Some((ck, entry)));
                }
                // If we've moved past the key entirely, stop.
                if ck.key > *key {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    /// Read all entries in the SSTable (for compaction/scanning).
    pub async fn read_all<K, TS>(
        &self,
    ) -> Result<Vec<(CompositeKey<K, TS>, LsmEntry)>, StorageError>
    where
        K: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        let index = self.read_index::<K, TS>().await?;
        let mut all = Vec::new();
        for idx_entry in &index {
            let entries = self.read_block(idx_entry).await?;
            all.extend(entries);
        }
        Ok(all)
    }

    /// Verify integrity of all blocks (for scrubbing).
    pub async fn verify<K, TS>(&self) -> Result<(), StorageError>
    where
        K: for<'de> Deserialize<'de>,
        TS: for<'de> Deserialize<'de>,
    {
        let index = self.read_index::<K, TS>().await?;
        for idx_entry in &index {
            self.read_block(idx_entry).await?;
        }
        Ok(())
    }

    pub fn num_entries(&self) -> u64 {
        self.footer.num_entries
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn close(self) {
        self.io.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::disk_io::{BufferedIo, OpenFlags};
    use super::super::vlog::ValuePointer;
    use tempfile::NamedTempFile;

    fn test_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            direct: false,
        }
    }

    #[tokio::test]
    async fn write_and_read_sstable() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut entries = BTreeMap::new();
        for i in 0u64..10 {
            let ck = CompositeKey::new(format!("key-{i:03}"), i * 10);
            let entry = LsmEntry {
                value_ptr: Some(ValuePointer {
                    segment_id: 1,
                    offset: i * 100,
                    length: 50,
                }),
                last_read_ts: None,
            };
            entries.insert(ck, entry);
        }

        let count =
            SSTableWriter::write::<String, u64, BufferedIo>(&path, &entries, test_flags())
                .await
                .unwrap();
        assert_eq!(count, 10);

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();
        assert_eq!(reader.num_entries(), 10);

        // Point lookup.
        let target = CompositeKey::new("key-005".to_string(), 50);
        let result = reader.get::<String, u64>(&target).await.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().value_ptr.unwrap().offset,
            500
        );

        // Bloom filter check.
        let found = reader.may_contain_key(&"key-005".to_string()).await.unwrap();
        assert!(found);
    }

    #[tokio::test]
    async fn get_at_version() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut entries = BTreeMap::new();
        // Two versions of same key.
        entries.insert(
            CompositeKey::new("a".to_string(), 10u64),
            LsmEntry {
                value_ptr: Some(ValuePointer {
                    segment_id: 1,
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
                    segment_id: 1,
                    offset: 100,
                    length: 10,
                }),
                last_read_ts: None,
            },
        );

        SSTableWriter::write::<String, u64, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        // At ts=15, should find version at ts=10.
        let result = reader
            .get_at::<String, u64>(&"a".to_string(), &15)
            .await
            .unwrap();
        assert!(result.is_some());
        let (ck, _) = result.unwrap();
        assert_eq!(ck.timestamp.0, 10);

        // At ts=25, should find version at ts=20 (latest <= 25).
        let result = reader
            .get_at::<String, u64>(&"a".to_string(), &25)
            .await
            .unwrap();
        assert!(result.is_some());
        let (ck, _) = result.unwrap();
        assert_eq!(ck.timestamp.0, 20);
    }

    #[tokio::test]
    async fn read_all_entries() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut entries = BTreeMap::new();
        for i in 0u64..5 {
            entries.insert(
                CompositeKey::new(i, i * 10),
                LsmEntry {
                    value_ptr: None,
                    last_read_ts: None,
                },
            );
        }

        SSTableWriter::write::<u64, u64, BufferedIo>(&path, &entries, test_flags())
            .await
            .unwrap();

        let reader = SSTableReader::<BufferedIo>::open(path, test_flags())
            .await
            .unwrap();

        let all = reader.read_all::<u64, u64>().await.unwrap();
        assert_eq!(all.len(), 5);
    }
}
