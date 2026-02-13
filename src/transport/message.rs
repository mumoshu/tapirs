use std::fmt::Debug;

pub trait Message: Clone + Send + Sync + Debug + 'static {}

impl<T: Clone + Send + Sync + Debug + 'static> Message for T {}
