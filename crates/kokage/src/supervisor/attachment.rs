use std::sync::Arc;

use crate::supervisor::SupervisorHandle;

/// Identity of one supervised child carrying an attachment.
///
/// A path of these values identifies an attached child within a supervision
/// tree without consulting serializable supervisor snapshots.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
#[doc(hidden)]
pub struct AttachedChildIdentity {
    /// Child id within its direct supervisor.
    pub id: String,
    /// Monotonic identity of this membership within its direct supervisor.
    pub lineage: u64,
    /// Current restart generation of this child membership.
    pub generation: u64,
}

/// A typed attachment read from a running supervision tree.
///
/// Attachments are process-local values and are intentionally kept separate
/// from [`ChildSnapshot`](crate::supervisor::ChildSnapshot), including when the `serde`
/// feature is enabled.
#[derive(Clone)]
#[doc(hidden)]
pub struct AttachedChild<T> {
    path: Vec<AttachedChildIdentity>,
    attachment: Arc<T>,
    supervisor: Option<SupervisorHandle>,
}

impl<T> AttachedChild<T> {
    pub(crate) fn new(
        path: Vec<AttachedChildIdentity>,
        attachment: Arc<T>,
        supervisor: Option<SupervisorHandle>,
    ) -> Self {
        Self {
            path,
            attachment,
            supervisor,
        }
    }

    /// Returns the identity path from the sampled supervisor to this child.
    pub fn path(&self) -> &[AttachedChildIdentity] {
        &self.path
    }

    /// Returns the typed process-local attachment.
    pub fn attachment(&self) -> &Arc<T> {
        &self.attachment
    }

    /// Returns this child's nested supervisor handle, when it is a supervisor.
    ///
    /// The handle and attachment were captured from the same membership entry.
    pub fn supervisor(&self) -> Option<&SupervisorHandle> {
        self.supervisor.as_ref()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for AttachedChild<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachedChild")
            .field("path", &self.path)
            .field("attachment", &self.attachment)
            .field("supervisor", &self.supervisor.is_some())
            .finish()
    }
}
