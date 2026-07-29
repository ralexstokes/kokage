#![allow(unused_imports)]

use kokage::host::{
    ActorContext as _, ActorRunError as _, BoxError as _, ChildContext as _, ChildResult as _,
    ChildSpec as _, RawActor as _, RunnableActor as _, DEFAULT_SHUTDOWN_BOUND as _,
};
use kokage::observe::{
    ActorStats as _, ChildExitView as _, ChildMembershipView as _, ChildOutline as _,
    ChildSnapshot as _, ChildStateView as _, CompletionError as _, CompletionGuard as _,
    CompletionOutcome as _, LifecycleEvent as _, LifecycleEventKind as _,
    LifecyclePathSegment as _, LifecycleWatch as _, LifecycleWatchGuard as _, SnapshotRecvError as _,
    SupervisionOutline as _, SupervisorPathSegment as _, SupervisorSnapshot as _,
    SupervisorSnapshotReceiver as _, SupervisorStateView as _,
};

use kokage::ActorContext;
use kokage::ActorRunError;
use kokage::ActorStats;
use kokage::BoxError;
use kokage::ChildContext;
use kokage::ChildExitView;
use kokage::ChildMembershipView;
use kokage::ChildOutline;
use kokage::ChildResult;
use kokage::ChildSnapshot;
use kokage::ChildSpec;
use kokage::ChildStateView;
use kokage::CompletionError;
use kokage::CompletionGuard;
use kokage::CompletionOutcome;
use kokage::DEFAULT_SHUTDOWN_BOUND;
use kokage::LifecycleEvent;
use kokage::LifecycleEventKind;
use kokage::LifecyclePathSegment;
use kokage::LifecycleWatch;
use kokage::LifecycleWatchGuard;
use kokage::RawActor;
use kokage::RunnableActor;
use kokage::SnapshotRecvError;
use kokage::SupervisionOutline;
use kokage::SupervisorPathSegment;
use kokage::SupervisorSnapshot;
use kokage::SupervisorSnapshotReceiver;
use kokage::SupervisorStateView;

fn main() {}
