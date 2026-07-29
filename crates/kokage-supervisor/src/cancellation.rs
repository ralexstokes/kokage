use std::{
    fmt,
    sync::{
        Arc, Mutex, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::watch;

/// A cloneable cancellation token used throughout Kokage supervision trees.
///
/// Cancelling a token wakes tasks waiting on [`cancelled`](Self::cancelled).
/// Child tokens are cancelled when their parent is cancelled, while
/// cancelling a child does not affect its parent or siblings.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<CancellationNode>,
}

struct CancellationNode {
    cancelled: AtomicBool,
    changed: watch::Sender<bool>,
    children: Mutex<Vec<Weak<CancellationNode>>>,
}

impl CancellationNode {
    fn new() -> Arc<Self> {
        let (changed, _) = watch::channel(false);
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            changed,
            children: Mutex::new(Vec::new()),
        })
    }
}

impl CancellationToken {
    /// Creates a token in the non-cancelled state.
    pub fn new() -> Self {
        Self {
            inner: CancellationNode::new(),
        }
    }

    /// Cancels this token and all of its descendants.
    pub fn cancel(&self) {
        let mut pending = vec![Arc::clone(&self.inner)];
        while let Some(node) = pending.pop() {
            if node.cancelled.swap(true, Ordering::AcqRel) {
                continue;
            }
            node.changed.send_replace(true);
            let mut children = node.children.lock().unwrap_or_else(PoisonError::into_inner);
            pending.extend(children.iter().filter_map(Weak::upgrade));
            children.clear();
        }
    }

    /// Creates a child token linked to this token.
    ///
    /// A child observes cancellation of its parent, but cancelling the child
    /// does not cancel the parent.
    pub fn child_token(&self) -> Self {
        let child = Self::new();
        let parent_cancelled = {
            let mut children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if self.inner.cancelled.load(Ordering::Acquire) {
                true
            } else {
                children.retain(|child| child.strong_count() > 0);
                children.push(Arc::downgrade(&child.inner));
                false
            }
        };
        if parent_cancelled {
            child.cancel();
        }
        child
    }

    /// Waits until this token is cancelled.
    pub async fn cancelled(&self) {
        let mut changed = self.inner.changed.subscribe();
        while !self.is_cancelled() {
            changed
                .changed()
                .await
                .expect("cancellation node retains its sender");
        }
    }

    /// Returns whether this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn drop_guard(self) -> CancellationGuard {
        CancellationGuard(Some(self))
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("is_cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

pub(crate) struct CancellationGuard(Option<CancellationToken>);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if let Some(token) = self.0.take() {
            token.cancel();
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::CancellationToken;

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
    async fn child_created_after_parent_cancellation_starts_cancelled() {
        let parent = CancellationToken::new();
        parent.cancel();

        let child = parent.child_token();
        child.cancelled().await;
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn dropping_a_guard_cancels_its_token() {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();
        drop(guard);

        token.cancelled().await;
        assert!(token.is_cancelled());
    }
}
