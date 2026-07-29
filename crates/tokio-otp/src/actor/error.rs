use thiserror::Error;

/// Indicates that Tokio cancelled queued blocking work during runtime
/// shutdown before it could return a value.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("blocking task was cancelled during runtime shutdown")]
pub struct BlockingCancelled;

/// Indicates that an [`ActorContext::offload`](crate::ActorContext::offload)
/// future did not complete before its required deadline.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("actor offload deadline elapsed")]
pub struct OffloadDeadline;

/// Errors returned while validating a graph during build.
///
/// This type intentionally excludes failures from resolving refs against an
/// already-built graph. Hand-built declarations that perform both steps can
/// use an application error type that accepts both this error and
/// [`GraphLookupError`].
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphBuildError {
    /// The graph was built without any actors.
    #[error("graph must contain at least one actor")]
    EmptyGraph,
    /// Two actor implementations shared the same id.
    #[error("duplicate actor id `{actor_id}`")]
    #[non_exhaustive]
    DuplicateActorId {
        /// Actor id registered twice.
        actor_id: String,
    },
    /// An actor slot was opened but no implementation was registered.
    #[error("actor `{actor_id}` slot was opened but never filled")]
    #[non_exhaustive]
    MissingActor {
        /// Actor id without an implementation.
        actor_id: String,
    },
    /// A graph-wide or per-actor mailbox capacity was zero.
    #[error("mailbox capacity must be non-zero")]
    ZeroMailboxCapacity,
    /// The graph's configured name was empty.
    #[error("graph name must not be empty")]
    EmptyGraphName,
    /// An actor slot was opened with an empty id.
    #[error("actor id must not be empty")]
    EmptyActorId,
    /// An actor slot from a different graph builder was passed to `define`.
    #[error("actor slot belongs to a different graph builder")]
    ForeignSlot,
}

/// Errors returned when resolving graph members after build.
///
/// This type remains distinct from [`GraphBuildError`] because lookup only
/// happens after a graph has built successfully.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphLookupError {
    /// An actor ref does not belong to this graph.
    #[error("actor ref `{actor_id}` does not belong to this graph")]
    #[non_exhaustive]
    ForeignActorRef {
        /// Id carried by the foreign actor ref.
        actor_id: String,
    },
}

/// Error returned when an awaited send cannot reach an actor membership.
///
/// Awaited sends ride through mailbox capacity pressure, closed incarnations,
/// and restart windows. They fail only after the target membership has
/// terminated, or when its binding source has otherwise become unavailable.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
#[error("actor `{actor_id}` is unavailable")]
pub struct SendError {
    /// Stable id of the target actor.
    pub actor_id: String,
}

#[cfg(test)]
mod tests {
    use super::SendError;

    #[test]
    fn send_error_display_covers_every_unavailable_binding() {
        assert_eq!(
            SendError {
                actor_id: "worker".to_owned(),
            }
            .to_string(),
            "actor `worker` is unavailable"
        );
    }
}

/// Errors returned by [`ActorRef::try_send`](crate::ActorRef::try_send).
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum TrySendError {
    /// The target actor is currently unbound and a restart is expected.
    #[error("actor `{actor_id}` is not currently running")]
    #[non_exhaustive]
    NotRunning {
        /// Stable id of the target actor.
        actor_id: String,
    },
    /// The target actor's mailbox is full.
    #[error("mailbox for actor `{actor_id}` is full")]
    #[non_exhaustive]
    Full {
        /// Stable id of the target actor.
        actor_id: String,
    },
    /// The current incarnation's mailbox is closed while its membership
    /// disposition is still being resolved.
    #[error("mailbox for actor `{actor_id}` is closed")]
    #[non_exhaustive]
    Closed {
        /// Stable id of the target actor.
        actor_id: String,
    },
    /// The target membership has terminated and no restart is scheduled.
    #[error("actor `{actor_id}` has terminated")]
    #[non_exhaustive]
    Terminated {
        /// Stable id of the target actor.
        actor_id: String,
    },
}

/// Errors returned by [`ActorRef::call`](crate::ActorRef::call).
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum CallError {
    /// The request message could not be delivered.
    #[error(transparent)]
    Send(#[from] SendError),
    /// The timeout expired before the actor replied.
    #[error("call to actor `{actor_id}` timed out")]
    #[non_exhaustive]
    Timeout {
        /// Target actor id.
        actor_id: String,
    },
    /// The actor dropped the [`Reply`](crate::Reply) without answering.
    #[error("actor `{actor_id}` dropped the reply")]
    #[non_exhaustive]
    ReplyDropped {
        /// Target actor id.
        actor_id: String,
    },
}
