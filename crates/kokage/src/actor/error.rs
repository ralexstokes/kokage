use std::{error::Error, fmt};

use thiserror::Error;

/// Indicates that Tokio cancelled queued blocking work during runtime
/// shutdown before it could return a value.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("blocking task was cancelled during runtime shutdown")]
pub struct BlockingCancelled;

/// Indicates that a [`RawContext::offload`](crate::raw::RawContext::offload)
/// future did not complete before its required deadline.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[error("actor offload deadline elapsed")]
pub struct OffloadDeadline;

/// Error returned when a send does not accept its message.
///
/// Every send flavor uses this carrier and returns the rejected message. The
/// [`kind`](Self::kind) identifies whether an immediate send found no running
/// incarnation or no mailbox capacity, an awaited send reached terminal
/// membership, or a bounded send elapsed.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct SendError<M> {
    /// Stable id of the target actor.
    pub actor_id: String,
    /// Message that was not accepted.
    pub message: M,
    /// Reason the message was not accepted.
    pub kind: SendErrorKind,
}

impl<M> SendError<M> {
    /// Returns the message that was not accepted.
    pub fn into_message(self) -> M {
        self.message
    }

    /// Drops the message payload and returns a boxed delivery error.
    ///
    /// This is useful when an application error must be `Send + Sync` but the
    /// message itself is not `Sync`.
    pub fn into_boxed(self) -> crate::BoxError {
        Box::new(ErasedSendError {
            actor_id: self.actor_id,
            kind: self.kind,
        })
    }
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError")
            .field("actor_id", &self.actor_id)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl<M> fmt::Display for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt_with_actor(&self.actor_id, f)
    }
}

impl<M> Error for SendError<M> {}

/// Reason a send did not accept its message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SendErrorKind {
    /// The target actor has no live incarnation right now.
    ///
    /// A retry may succeed if its membership remains and another incarnation
    /// starts. This also covers the brief window while a closed incarnation's
    /// final disposition is being resolved.
    NotRunning,
    /// The target actor's mailbox is full.
    Full,
    /// The target membership has terminated and no restart is scheduled.
    Terminated,
    /// The message was not accepted before the bound elapsed.
    TimedOut,
}

impl SendErrorKind {
    fn fmt_with_actor(self, actor_id: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRunning => write!(f, "actor `{actor_id}` is not currently running"),
            Self::Full => write!(f, "mailbox for actor `{actor_id}` is full"),
            Self::Terminated => write!(f, "actor `{actor_id}` has terminated"),
            Self::TimedOut => {
                write!(f, "send to actor `{actor_id}` timed out")
            }
        }
    }
}

#[derive(Debug)]
struct ErasedSendError {
    actor_id: String,
    kind: SendErrorKind,
}

impl fmt::Display for ErasedSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt_with_actor(&self.actor_id, f)
    }
}

impl Error for ErasedSendError {}

/// Errors returned by [`ActorRef::call`](crate::ActorRef::call).
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum CallError {
    /// The target actor terminated before accepting the request.
    #[error("actor `{actor_id}` terminated before accepting the call")]
    #[non_exhaustive]
    Terminated {
        /// Target actor id.
        actor_id: String,
    },
    /// The timeout expired before the request entered the mailbox.
    ///
    /// The request was not accepted, so retrying it cannot duplicate work.
    #[error("call to actor `{actor_id}` timed out before acceptance")]
    #[non_exhaustive]
    AcceptanceTimedOut {
        /// Target actor id.
        actor_id: String,
    },
    /// The timeout expired after the request entered the mailbox.
    ///
    /// The actor may still process the request, so its outcome is unknown.
    #[error("call to actor `{actor_id}` timed out waiting for a response")]
    #[non_exhaustive]
    ResponseTimedOut {
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

/// Errors returned by a lower-level [`ReplyReceiver`](crate::ReplyReceiver).
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplyError {
    /// Every reply sender was dropped without answering.
    #[error("reply sender was dropped without answering")]
    Dropped,
    /// The requested reply deadline elapsed.
    #[error("reply deadline elapsed")]
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::{SendError, SendErrorKind};

    struct Opaque;

    #[test]
    fn generic_delivery_errors_do_not_format_the_message() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<SendError<Opaque>>();

        for (kind, display) in [
            (
                SendErrorKind::NotRunning,
                "actor `worker` is not currently running",
            ),
            (SendErrorKind::Full, "mailbox for actor `worker` is full"),
            (SendErrorKind::Terminated, "actor `worker` has terminated"),
            (SendErrorKind::TimedOut, "send to actor `worker` timed out"),
        ] {
            let send = SendError {
                actor_id: "worker".to_owned(),
                message: Opaque,
                kind,
            };
            assert_eq!(send.to_string(), display);
            assert_eq!(
                format!("{send:?}"),
                format!("SendError {{ actor_id: \"worker\", kind: {kind:?}, .. }}")
            );
        }
    }

    #[test]
    fn payload_erasure_retains_the_target_and_rejection() {
        for kind in [
            SendErrorKind::NotRunning,
            SendErrorKind::Full,
            SendErrorKind::Terminated,
            SendErrorKind::TimedOut,
        ] {
            let error = SendError {
                actor_id: "worker".to_owned(),
                message: Opaque,
                kind,
            };
            let carrier_display = error.to_string();
            let erased = error.into_boxed();
            assert_eq!(erased.to_string(), carrier_display);
        }
    }
}
