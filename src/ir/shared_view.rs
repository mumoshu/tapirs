use super::View;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

/// Arc-wrapped View for cheap cloning. Serializes/deserializes as plain View.
#[derive(Clone, Debug)]
pub struct SharedView<A>(Arc<View<A>>);

impl<A> SharedView<A> {
    pub fn new(view: View<A>) -> Self {
        Self(Arc::new(view))
    }

    pub fn make_mut(&mut self) -> &mut View<A>
    where
        A: Clone,
    {
        Arc::make_mut(&mut self.0)
    }
}

impl<A> std::ops::Deref for SharedView<A> {
    type Target = View<A>;
    fn deref(&self) -> &View<A> {
        &self.0
    }
}

impl<A: Serialize> Serialize for SharedView<A> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, A: Deserialize<'de>> Deserialize<'de> for SharedView<A> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        View::<A>::deserialize(deserializer).map(Self::new)
    }
}
