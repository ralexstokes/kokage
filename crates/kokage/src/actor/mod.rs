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

pub use binding::{ActorStats, MailboxMode, SupervisorPathSegment};
pub(crate) use builder::ActorOptionsValidationError;
pub use builder::{ActorNode, ActorSlot, ActorSpec, GraphBuilder};
pub(crate) use cancellation::CancelOnDrop;
pub use cancellation::CancellationHandle;
pub use context::{
    ActorContext, ActorRef, ActorStatus, DynamicRestrictedScope, LiveContext, MessageContext,
    Reply, RestrictedScope, StartContext, StopContext, TaskHandle, TimerKey,
};
pub use error::{
    BlockingCancelled, CallError, GraphBuildError, GraphSpawnError, OffloadDeadline, SendError,
    TrySendError,
};
pub use factory::ActorFactory;
pub(crate) use graph::RunnableActorBuilder;
pub use graph::{ActorRunError, DEFAULT_SHUTDOWN_BOUND, Graph, RunnableActor};
pub use handler::{Actor, DrainPolicy};
pub use monitor::{DownReason, MonitorEvent};
pub use raw::{ActorResult, RawActor};
