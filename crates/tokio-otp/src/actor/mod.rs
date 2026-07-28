//! Actor layer: typed actor graphs with restart-stable refs.
//!
//! This module tree is private; its public API is re-exported flat from the
//! crate root. The terminate-binding drop guard connecting the actor and
//! runtime layers lives in `crate::runtime` as a crate-internal invariant.

mod binding;
mod builder;
mod cancellation;
mod context;
mod error;
mod factory;
mod graph;
mod handler;
mod monitor;
mod observability;
mod raw;

pub(crate) use context::deadline_after;

pub use binding::{ActorStats, MailboxMode, SupervisorPathSegment};
pub(crate) use builder::ActorOptionsValidationError;
pub use builder::{ActorOptions, ActorSlot, GraphBuilder, GraphConfig, MessageSize};
pub use cancellation::{CancellationHandle, Lifetime};
pub use context::{
    ActorContext, ActorRef, AmbientContext, LiveContext, MessageContext, OffloadHandle, Reply,
    RestrictedScope, StartContext, StopContext, TimerKey,
};
pub use error::{
    BlockingCancelled, CallError, GraphBuildError, GraphLookupError, OffloadDeadline, SendError,
    TrySendError,
};
pub use factory::ActorFactory;
pub(crate) use graph::RunnableActorBuilder;
pub use graph::{ActorRunError, DEFAULT_SHUTDOWN_BOUND, Graph, RunnableActor};
pub use handler::{Actor, DrainPolicy};
pub use monitor::{Down, DownReason, MonitorEvent};
pub use raw::{ActorResult, RawActor};
