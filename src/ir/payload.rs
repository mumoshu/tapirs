use super::ViewNumber;

pub trait IrPayload: Clone + std::fmt::Debug + Send + 'static {
    type Record;
    type RawRecord;
    fn base_view(&self) -> Option<ViewNumber>;
    /// Return a raw record for iteration over this payload's entries.
    /// The raw record supports iteration only (RecordIter), not point lookups.
    /// Use `into_indexed()` on the result to get a PersistentRecord with lookups.
    fn as_raw_record(&self) -> Self::RawRecord;
}
