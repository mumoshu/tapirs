use futures::{stream::FuturesUnordered, Stream};
use pin_project_lite::pin_project;
use std::{
    collections::BTreeMap,
    future::Future,
    mem,
    pin::Pin,
    task::{ready, Context, Poll},
};

pin_project! {
    /// Future for the [`join`] function.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Join<K, F: Future> {
        #[pin]
        active: FuturesUnordered<KeyedFuture<K, F>>,
        // BTreeMap instead of HashMap for deterministic iteration order. HashMap's
        // RandomState seeds from OS entropy, making iteration order vary across
        // process runs even with seeded protocol RNG. BTreeMap's O(log n) overhead
        // vs HashMap's O(1) is negligible — this collection holds at most 2f+1
        // entries (typically 3-7 replicas).
        output: BTreeMap<K, F::Output>,
    }
}

pin_project! {
    /// Future for the [`until`] function.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct JoinUntil<K, F: Future, U: Until<K, F::Output>> {
        #[pin]
        active: FuturesUnordered<KeyedFuture<K, F>>,
        output: BTreeMap<K, F::Output>,
        until: U
    }
}

pub trait Until<K, O> {
    fn until(&mut self, results: &BTreeMap<K, O>, cx: &mut Context<'_>) -> bool;
}

impl<K, O> Until<K, O> for usize {
    fn until(&mut self, results: &BTreeMap<K, O>, _cx: &mut Context<'_>) -> bool {
        results.len() >= *self
    }
}

impl<K, O, F: FnMut(&BTreeMap<K, O>, &mut Context<'_>) -> bool> Until<K, O> for F {
    fn until(&mut self, results: &BTreeMap<K, O>, cx: &mut Context<'_>) -> bool {
        self(results, cx)
    }
}

pin_project! {
    struct KeyedFuture<K, F> {
        #[pin]
        future: F,
        key: Option<K>,
    }
}

impl<K, F: Future> Future for KeyedFuture<K, F> {
    type Output = (K, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let result = ready!(this.future.as_mut().poll(cx));
        Poll::Ready((this.key.take().unwrap(), result))
    }
}

pub fn join<K, F, I: IntoIterator<Item = (K, F)>>(iter: I) -> Join<K, F>
where
    F: Future,
{
    let active = FuturesUnordered::default();
    for (key, future) in iter {
        active.push(KeyedFuture {
            key: Some(key),
            future,
        });
    }

    Join {
        output: BTreeMap::new(),
        active,
    }
}

impl<K: Ord, F> Join<K, F>
where
    F: Future,
{
    pub(crate) fn until<U: Until<K, F::Output>>(self, until: U) -> JoinUntil<K, F, U> {
        JoinUntil {
            active: self.active,
            output: self.output,
            until,
        }
    }
}

impl<K: Ord, F, U: Until<K, F::Output>> Future for JoinUntil<K, F, U>
where
    F: Future,
{
    type Output = BTreeMap<K, F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            if !this.until.until(this.output, cx) {
                match this.active.as_mut().poll_next(cx) {
                    Poll::Ready(Some((k, x))) => {
                        this.output.insert(k, x);
                        continue;
                    }
                    Poll::Ready(None) => {
                        // Done with all futures
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            return Poll::Ready(mem::take(this.output));
        }
    }
}
