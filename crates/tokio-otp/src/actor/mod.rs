//! Actor layer: typed actor graphs with restart-stable refs (formerly the
//! `tokio-actor` crate).
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
pub use builder::{ActorOptions, ActorSlot, GraphBuilder, MessageSize};
pub use cancellation::CancellationHandle;
pub use context::{ActorContext, ActorRef, Reply, StepHandle};
pub use error::{CallError, GraphBuildError, SendError, StepDeadline, TryRecvError};
pub use factory::ActorFactory;
pub use graph::{ActorRunError, Graph, RunnableActor, RunnableActorFactory};
pub use handler::{Actor, DrainPolicy};
pub use monitor::{Down, DownReason, MonitorEvent};
pub use raw::{ActorResult, BoxError, Flow, RawActor};
