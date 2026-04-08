use super::ViewNumber;

pub trait IrPayload: Clone + std::fmt::Debug + Send + 'static {
    type Record;
    fn base_view(&self) -> Option<ViewNumber>;
    /// Return a Record containing only this payload's own segment entries,
    /// without resolving against a base. For delta payloads this contains
    /// only the delta entries; for full payloads, all entries.
    fn as_unresolved_record(&self) -> Self::Record;
}
