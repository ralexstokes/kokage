use thiserror::Error;

/// Indicates that an [`ActorContext::offload`](crate::ActorContext::offload)
/// future did not complete before its required deadline.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("actor offload deadline elapsed")]
pub struct OffloadDeadline;

/// Errors returned by [`ActorContext::try_recv`](crate::ActorContext::try_recv).
///
/// This crate-owned type keeps the actor mailbox API independent of Tokio's
/// channel error types. It describes the actor-level states that callers can
/// act on regardless of the mailbox implementation selected for an actor. The
/// enum is intentionally exhaustive: future mailbox implementations must map
/// their no-message and terminal states into this stable actor-level contract.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum TryRecvError {
    /// No message is immediately available, but the mailbox may receive more.
    #[error("actor mailbox is empty")]
    Empty,
    /// The mailbox is closed and cannot receive more messages.
    #[error("actor mailbox is disconnected")]
    Disconnected,
}

/// Errors returned while validating a graph during build.
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
    /// Generic invalid builder configuration.
    #[error("{0}")]
    InvalidConfig(&'static str),
}

/// Errors returned when sending to an actor mailbox.
///
/// The variants form sender-visible lifecycle vocabulary. `MailboxFull` is
/// transient backpressure from [`ActorRef::try_send`](crate::ActorRef::try_send).
/// `ActorNotRunning` means the membership expects another incarnation, so an
/// awaited [`ActorRef::send`](crate::ActorRef::send) waits for that rebind.
/// `ActorTerminated` means the membership is terminal and will never rebind;
/// removing and re-adding the same actor id creates a different membership.
/// `MailboxClosed` is a race observed by non-waiting sends when the current
/// incarnation has closed intake but its final lifecycle disposition is not
/// visible yet.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum SendError {
    /// The target actor is currently unbound and a restart is expected.
    #[error("actor `{actor_id}` is not currently running")]
    #[non_exhaustive]
    ActorNotRunning {
        /// Stable id of the target actor.
        actor_id: String,
    },
    /// The target membership has terminated and no restart is scheduled.
    #[error("actor `{actor_id}` has terminated")]
    #[non_exhaustive]
    ActorTerminated {
        /// Stable id of the target actor.
        actor_id: String,
    },
    /// The target actor's mailbox is full.
    #[error("mailbox for actor `{actor_id}` is full")]
    #[non_exhaustive]
    MailboxFull {
        /// Stable id of the target actor.
        actor_id: String,
    },
    /// The current incarnation's mailbox is closed while its membership
    /// disposition is still being resolved.
    #[error("mailbox for actor `{actor_id}` is closed")]
    #[non_exhaustive]
    MailboxClosed {
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
