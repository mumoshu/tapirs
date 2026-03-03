use serde::{Deserialize, Serialize};

/// Physical pointer to an entry within a VLog segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VlogPtr {
    pub segment_id: u64,
    pub offset: u64,
    pub length: u32,
}

/// Byte range and entry count for one view's entries within a VLog segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRange {
    pub view: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub num_entries: u32,
}

/// Metadata for a sealed VLog segment stored in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VlogSegmentMeta {
    pub segment_id: u64,
    pub path: std::path::PathBuf,
    pub views: Vec<ViewRange>,
    pub total_size: u64,
}
