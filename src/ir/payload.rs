use super::ViewNumber;

pub trait IrPayload: Clone + std::fmt::Debug + Send + 'static {
    type Record;
    fn base_view(&self) -> Option<ViewNumber>;
    /// Return a Record containing only this payload's own segment entries,
    /// without resolving against a base. For delta payloads this contains
    /// only the delta entries; for full payloads, all entries.
    fn as_unresolved_record(&self) -> Self::Record;

    /// Return a Record containing only segments whose max view > base_view.
    /// Extracts missed-view sealed entries from full payloads. Memtable
    /// segments (empty ViewRange) are excluded since max_view(empty) = 0.
    /// For delta payloads where the segment's view <= base_view (common
    /// case: no missed views), returns an empty record.
    fn as_record_since(&self, base_view: u64) -> Self::Record;

    /// Return a Record containing only memtable segments.
    /// For delta payloads, the entire payload is the memtable.
    /// For full payloads (base_view == 0), only segments with empty
    /// ViewRange are included — these are the memtable entries appended
    /// by build_full_view_change_payload.
    fn as_memtable_record(&self) -> Self::Record;
}
