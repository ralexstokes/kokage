use std::{error::Error, fmt};

use thiserror::Error;

/// Indicates that Tokio cancelled queued blocking work during runtime
/// shutdown before it could return a value.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("blocking task was cancelled during runtime shutdown")]
pub struct BlockingCancelled;

/// Indicates that a [`RawContext::offload`](crate::host::RawContext::offload)
/// future did not complete before its required deadline.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("actor offload deadline elapsed")]
pub struct OffloadDeadline;

/// Error returned when an awaited send cannot reach an actor membership.
///
/// Awaited sends ride through mailbox capacity pressure, closed incarnations,
/// and restart windows. They fail only after the target membership has
/// terminated, or when its binding source has otherwise become unavailable.
/// The rejected message remains available in [`message`](Self::message) or
/// through [`into_message`](Self::into_message).
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct SendError<M> {
    /// Stable id of the target actor.
    pub actor_id: String,
    /// Message that was not accepted.
    pub message: M,
}

impl<M> SendError<M> {
    /// Returns the message that was not accepted.
    pub fn into_message(self) -> M {
        self.message
    }

    /// Drops the message payload and returns a non-generic rejection.
    ///
    /// This is useful when an application error must be `Send + Sync` but the
    /// message itself is not `Sync`.
    pub fn discard(self) -> SendRejection {
        SendRejection::Terminated {
            actor_id: self.actor_id,
        }
    }
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError")
            .field("actor_id", &self.actor_id)
            .finish_non_exhaustive()
    }
}

impl<M> fmt::Display for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor `{}` has terminated", self.actor_id)
    }
}

impl<M> Error for SendError<M> {}

/// Errors returned by [`ActorRef::try_send`](crate::ActorRef::try_send).
///
/// Every variant owns the rejected message. Use [`into_message`](Self::into_message)
/// to retry or reroute it, or [`discard`](Self::discard) when only the rejection
/// reason is needed.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum TrySendError<M> {
    /// The target actor has no live incarnation right now.
    ///
    /// A retry may succeed if its membership remains and another incarnation
    /// starts. This also covers the brief window while a closed incarnation's
    /// final disposition is being resolved.
    #[non_exhaustive]
    NotRunning {
        /// Stable id of the target actor.
        actor_id: String,
        /// Message that was not accepted.
        message: M,
    },
    /// The target actor's mailbox is full.
    #[non_exhaustive]
    Full {
        /// Stable id of the target actor.
        actor_id: String,
        /// Message that was not accepted.
        message: M,
    },
    /// The target membership has terminated and no restart is scheduled.
    #[non_exhaustive]
    Terminated {
        /// Stable id of the target actor.
        actor_id: String,
        /// Message that was not accepted.
        message: M,
    },
}

impl<M> TrySendError<M> {
    /// Returns the message that was not accepted.
    pub fn into_message(self) -> M {
        match self {
            Self::NotRunning { message, .. }
            | Self::Full { message, .. }
            | Self::Terminated { message, .. } => message,
        }
    }

    /// Drops the message payload and returns a non-generic rejection.
    pub fn discard(self) -> SendRejection {
        match self {
            Self::NotRunning { actor_id, .. } => SendRejection::NotRunning { actor_id },
            Self::Full { actor_id, .. } => SendRejection::Full { actor_id },
            Self::Terminated { actor_id, .. } => SendRejection::Terminated { actor_id },
        }
    }
}

impl<M> fmt::Debug for TrySendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRunning { actor_id, .. } => f
                .debug_struct("NotRunning")
                .field("actor_id", actor_id)
                .finish_non_exhaustive(),
            Self::Full { actor_id, .. } => f
                .debug_struct("Full")
                .field("actor_id", actor_id)
                .finish_non_exhaustive(),
            Self::Terminated { actor_id, .. } => f
                .debug_struct("Terminated")
                .field("actor_id", actor_id)
                .finish_non_exhaustive(),
        }
    }
}

impl<M> fmt::Display for TrySendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRunning { actor_id, .. } => {
                write!(f, "actor `{actor_id}` is not currently running")
            }
            Self::Full { actor_id, .. } => write!(f, "mailbox for actor `{actor_id}` is full"),
            Self::Terminated { actor_id, .. } => {
                write!(f, "actor `{actor_id}` has terminated")
            }
        }
    }
}

impl<M> Error for TrySendError<M> {}

/// Error returned by [`ActorRef::send_timeout`](crate::ActorRef::send_timeout).
///
/// Unlike applying [`tokio::time::timeout`] to
/// [`ActorRef::send`](crate::ActorRef::send), this error always returns a
/// message that was not accepted before the bound elapsed.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum SendTimeoutError<M> {
    /// The message was not accepted before the bound elapsed.
    #[non_exhaustive]
    Timeout {
        /// Stable id of the target actor.
        actor_id: String,
        /// Message that was not accepted.
        message: M,
    },
    /// The target membership terminated before accepting the message.
    #[non_exhaustive]
    Terminated {
        /// Stable id of the target actor.
        actor_id: String,
        /// Message that was not accepted.
        message: M,
    },
}

impl<M> SendTimeoutError<M> {
    /// Returns the message that was not accepted.
    pub fn into_message(self) -> M {
        match self {
            Self::Timeout { message, .. } | Self::Terminated { message, .. } => message,
        }
    }

    /// Drops the message payload and returns a non-generic rejection.
    ///
    /// This is useful when an application error must be `Send + Sync` but the
    /// message itself is not `Sync`.
    pub fn discard(self) -> SendRejection {
        match self {
            Self::Timeout { actor_id, .. } => SendRejection::TimedOut { actor_id },
            Self::Terminated { actor_id, .. } => SendRejection::Terminated { actor_id },
        }
    }
}

impl<M> fmt::Debug for SendTimeoutError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { actor_id, .. } => f
                .debug_struct("Timeout")
                .field("actor_id", actor_id)
                .finish_non_exhaustive(),
            Self::Terminated { actor_id, .. } => f
                .debug_struct("Terminated")
                .field("actor_id", actor_id)
                .finish_non_exhaustive(),
        }
    }
}

impl<M> fmt::Display for SendTimeoutError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { actor_id, .. } => {
                write!(f, "send to actor `{actor_id}` timed out")
            }
            Self::Terminated { actor_id, .. } => {
                write!(f, "actor `{actor_id}` has terminated")
            }
        }
    }
}

impl<M> Error for SendTimeoutError<M> {}

/// A delivery rejection without the rejected message.
///
/// Use [`SendError::discard`], [`TrySendError::discard`], or
/// [`SendTimeoutError::discard`] when an application error needs the reason
/// and target id but must not inherit the message's `Send` or `Sync` bounds.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum SendRejection {
    /// The target actor has no live incarnation right now.
    ///
    /// A retry may succeed if its membership remains and another incarnation
    /// starts. This also covers the brief window while a closed incarnation's
    /// final disposition is being resolved.
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
    /// The message was not accepted before the delivery bound elapsed.
    #[error("send to actor `{actor_id}` timed out")]
    #[non_exhaustive]
    TimedOut {
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
    ///
    /// Calls use an awaited send internally, so the current implementation
    /// produces only [`SendRejection::Terminated`] here. Other delivery
    /// rejections remain part of the non-exhaustive carrier for composition.
    #[error(transparent)]
    Send(#[from] SendRejection),
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

#[cfg(test)]
mod tests {
    use super::{SendError, SendRejection, SendTimeoutError, TrySendError};

    struct Opaque;

    #[test]
    fn generic_delivery_errors_do_not_format_the_message() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<SendError<Opaque>>();
        assert_error::<TrySendError<Opaque>>();
        assert_error::<SendTimeoutError<Opaque>>();

        let send = SendError {
            actor_id: "worker".to_owned(),
            message: Opaque,
        };
        assert_eq!(send.to_string(), "actor `worker` has terminated");
        assert_eq!(
            format!("{send:?}"),
            "SendError { actor_id: \"worker\", .. }"
        );

        let try_send = TrySendError::Full {
            actor_id: "worker".to_owned(),
            message: Opaque,
        };
        assert_eq!(try_send.to_string(), "mailbox for actor `worker` is full");
        assert_eq!(format!("{try_send:?}"), "Full { actor_id: \"worker\", .. }");

        let timed = SendTimeoutError::Timeout {
            actor_id: "worker".to_owned(),
            message: Opaque,
        };
        assert_eq!(timed.to_string(), "send to actor `worker` timed out");
        assert_eq!(format!("{timed:?}"), "Timeout { actor_id: \"worker\", .. }");
    }

    #[test]
    fn discarding_retains_the_target_and_rejection() {
        let rejection = TrySendError::NotRunning {
            actor_id: "worker".to_owned(),
            message: Opaque,
        }
        .discard();
        assert_eq!(
            rejection,
            SendRejection::NotRunning {
                actor_id: "worker".to_owned()
            }
        );

        let rejection = SendError {
            actor_id: "worker".to_owned(),
            message: Opaque,
        }
        .discard();
        assert_eq!(
            rejection,
            SendRejection::Terminated {
                actor_id: "worker".to_owned()
            }
        );

        let rejection = SendTimeoutError::Timeout {
            actor_id: "worker".to_owned(),
            message: Opaque,
        }
        .discard();
        assert_eq!(
            rejection,
            SendRejection::TimedOut {
                actor_id: "worker".to_owned()
            }
        );
    }
}
