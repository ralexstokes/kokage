//! Actor layer: typed declarations with restart-stable refs.
//!
//! This module tree is private; its public API is re-exported flat from the
//! crate root. The terminate-binding drop guard connecting the actor and
//! runtime layers lives in `crate::runtime` as a crate-internal invariant.

mod binding;
mod builder;
mod context;
mod error;
mod factory;
mod graph;
mod handler;
mod monitor;
mod observability;
mod raw;

pub use binding::{ActorStats, Mailbox, ScopedActorStats};
pub(crate) use builder::{ActorNode, ActorOptionsValidationError};
pub use builder::{ActorSlot, ActorSpec};
pub use context::{ActorRef, Context, RawContext, Reply, ReplyReceiver, StopContext, TimerKey};
pub use error::{
    BlockingCancelled, CallError, OffloadDeadline, ReplyError, SendError, SendErrorKind,
};
pub use factory::ActorFactory;
#[cfg(feature = "host")]
pub use graph::{ActorHost, ActorRunError, DEFAULT_SHUTDOWN_BOUND, IncarnationExit};
pub(crate) use graph::{DEFAULT_MAILBOX_CAPACITY, RunnableActor, RunnableActorBuilder};
pub use handler::Actor;
pub use monitor::{MonitorEvent, MonitorEventKind};
pub use raw::{ExitResult, RawActor};
