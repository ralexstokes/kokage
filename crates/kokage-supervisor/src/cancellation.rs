use std::future::Future;

use tokio::sync::watch;

/// A cloneable cancellation token used throughout Kokage supervision trees.
///
/// Cancelling a token wakes tasks waiting on [`cancelled`](Self::cancelled).
/// Child tokens are cancelled when their parent is cancelled, while
/// cancelling a child does not affect its parent or siblings.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: tokio_util::sync::CancellationToken,
    liveness: watch::Sender<()>,
}

impl CancellationToken {
    /// Creates a token in the non-cancelled state.
    pub fn new() -> Self {
        let (liveness, _) = watch::channel(());
        Self {
            inner: tokio_util::sync::CancellationToken::new(),
            liveness,
        }
    }

    /// Cancels this token and all of its descendants.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Cancels this token when `future` completes.
    ///
    /// This links an external shutdown signal to the token without exposing
    /// the signal's runtime or concrete future type in Kokage's public API.
    /// If the token is cancelled first, the linking task stops polling and
    /// drops `future`.
    ///
    /// The linking task is detached and runs on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn cancel_when<F>(&self, future: F)
    where
        F: Future + Send + 'static,
    {
        let token = self.inner.clone();
        let cancellation = self.inner.clone();
        let mut liveness = self.liveness.subscribe();
        std::mem::drop(tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {}
                _ = liveness.changed() => {}
                _ = future => token.cancel(),
            }
        }));
    }

    /// Creates a child token linked to this token.
    ///
    /// A child observes cancellation of its parent, but cancelling the child
    /// does not cancel the parent. Child tokens also keep the parent's
    /// [`cancel_when`](Self::cancel_when) links alive.
    pub fn child_token(&self) -> Self {
        Self {
            inner: self.inner.child_token(),
            liveness: self.liveness.clone(),
        }
    }

    /// Waits until this token is cancelled.
    pub async fn cancelled(&self) {
        self.inner.cancelled().await;
    }

    /// Returns whether this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    pub(crate) fn drop_guard(self) -> impl Drop {
        self.inner.drop_guard()
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use super::CancellationToken;
    use tokio::sync::oneshot;

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(dropped) = self.0.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[tokio::test]
    async fn cancellation_tree_preserves_parent_child_direction() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let sibling = parent.child_token();

        child.cancel();
        child.cancelled().await;
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
        assert!(!sibling.is_cancelled());

        parent.cancel();
        sibling.cancelled().await;
        assert!(parent.is_cancelled());
        assert!(sibling.is_cancelled());
    }

    #[tokio::test]
    async fn child_token_keeps_parent_cancel_when_link_alive() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let (signal, signalled) = oneshot::channel();
        parent.cancel_when(async move {
            signalled.await.expect("signal sender was dropped");
        });
        drop(parent);

        signal.send(()).expect("linking task remains alive");
        tokio::time::timeout(Duration::from_secs(1), child.cancelled())
            .await
            .expect("parent link cancels its live child");
    }

    #[tokio::test]
    async fn cancel_when_links_an_arbitrary_future_to_the_token() {
        let token = CancellationToken::new();
        let (signal, signalled) = oneshot::channel();

        token.cancel_when(async move {
            signalled.await.expect("signal sender was dropped");
            "future outputs do not have to be unit"
        });
        assert!(!token.is_cancelled());

        signal.send(()).expect("linking task dropped its receiver");
        token.cancelled().await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn cancelling_the_token_drops_a_pending_linked_future() {
        let token = CancellationToken::new();
        let (dropped, was_dropped) = oneshot::channel();
        let drop_signal = DropSignal(Some(dropped));
        token.cancel_when(async move {
            let _drop_signal = drop_signal;
            std::future::pending::<()>().await;
        });

        token.cancel();
        tokio::time::timeout(Duration::from_secs(1), was_dropped)
            .await
            .expect("linked future was not dropped after cancellation")
            .expect("drop signal sender disappeared without running Drop");
    }

    #[tokio::test]
    async fn an_already_cancelled_token_drops_the_future_without_polling_it() {
        let token = CancellationToken::new();
        token.cancel();

        let was_polled = Arc::new(AtomicBool::new(false));
        let linked_was_polled = Arc::clone(&was_polled);
        let (dropped, was_dropped) = oneshot::channel();
        let drop_signal = DropSignal(Some(dropped));
        token.cancel_when(async move {
            let _drop_signal = drop_signal;
            std::future::poll_fn(move |_| {
                linked_was_polled.store(true, Ordering::Relaxed);
                std::task::Poll::<()>::Pending
            })
            .await;
        });

        tokio::time::timeout(Duration::from_secs(1), was_dropped)
            .await
            .expect("linked future was not dropped")
            .expect("drop signal sender disappeared without running Drop");
        assert!(!was_polled.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn dropping_the_last_token_drops_a_pending_linked_future() {
        let token = CancellationToken::new();
        let (dropped, was_dropped) = oneshot::channel();
        let drop_signal = DropSignal(Some(dropped));
        token.cancel_when(async move {
            let _drop_signal = drop_signal;
            std::future::pending::<()>().await;
        });

        drop(token);

        tokio::time::timeout(Duration::from_secs(1), was_dropped)
            .await
            .expect("linked future was not dropped with the last token")
            .expect("drop signal sender disappeared without running Drop");
    }
}
