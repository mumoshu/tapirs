use super::ViewNumber;

pub trait IrPayload: Clone + std::fmt::Debug + Send + 'static {
    type Record;
    fn resolve(self, base: Option<&Self::Record>) -> Self::Record;
    fn base_view(&self) -> Option<ViewNumber>;
}
