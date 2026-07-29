/// A cloneable cancellation token used throughout Kokage supervision trees.
///
/// Cancelling a token wakes tasks waiting on [`cancelled`](Self::cancelled).
/// Child tokens are cancelled when their parent is cancelled, while
/// cancelling a child does not affect its parent or siblings.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: tokio_util::sync::CancellationToken,
}

impl CancellationToken {
    /// Creates a token in the non-cancelled state.
    pub fn new() -> Self {
        Self {
            inner: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// Cancels this token and all of its descendants.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Creates a child token linked to this token.
    ///
    /// A child observes cancellation of its parent, but cancelling the child
    /// does not cancel the parent.
    pub fn child_token(&self) -> Self {
        Self {
            inner: self.inner.child_token(),
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
}
