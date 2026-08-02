use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{
    BoxError, ChildEventKind, ChildSnapshot, ExitStatus, LifecycleEvent, LifecycleEventKind,
    LifecycleWatch, RestartPolicy, SupervisorError, SupervisorHandle, SupervisorSnapshot,
    SupervisorSnapshotReceiver,
};
use tokio::{sync::mpsc, time::timeout};

pub const EVENT_TIMEOUT: Duration = Duration::from_secs(2);
pub const QUIET_TIMEOUT: Duration = Duration::from_millis(150);
pub const SHORT_GRACE: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExitStatusView {
    Completed,
    Failed(String),
    ReadinessTimedOut(Duration),
    Panicked,
    Aborted { after_grace: bool },
}

impl From<ExitStatus> for ExitStatusView {
    fn from(exit: ExitStatus) -> Self {
        match exit {
            ExitStatus::Completed { .. } => Self::Completed,
            ExitStatus::Failed { message, .. } => Self::Failed(message),
            ExitStatus::ReadinessTimedOut { timeout, .. } => Self::ReadinessTimedOut(timeout),
            ExitStatus::Panicked { .. } => Self::Panicked,
            ExitStatus::Aborted { after_grace, .. } => Self::Aborted { after_grace },
        }
    }
}

pub fn restart_with_backoff(
    max_restarts: usize,
    within: Duration,
    backoff: crate::supervisor::Backoff,
) -> RestartPolicy {
    RestartPolicy::on_failure()
        .limit(max_restarts, within)
        .backoff(backoff)
}

pub fn test_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

pub async fn recv_event<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T
where
    T: Debug,
{
    timeout(EVENT_TIMEOUT, rx.recv())
        .await
        .expect("timed out waiting for channel event")
        .expect("channel closed before expected event arrived")
}

pub async fn recv_bounded_event<T>(rx: &mut mpsc::Receiver<T>) -> T
where
    T: Debug,
{
    timeout(EVENT_TIMEOUT, rx.recv())
        .await
        .expect("timed out waiting for bounded channel event")
        .expect("bounded channel closed before expected event arrived")
}

pub async fn recv_n<T>(rx: &mut mpsc::UnboundedReceiver<T>, n: usize) -> Vec<T>
where
    T: Debug,
{
    let mut items = Vec::with_capacity(n);
    for _ in 0..n {
        items.push(recv_event(rx).await);
    }
    items
}

pub async fn assert_no_event<T>(rx: &mut mpsc::UnboundedReceiver<T>)
where
    T: Debug,
{
    if let Ok(Some(value)) = timeout(QUIET_TIMEOUT, rx.recv()).await {
        panic!("unexpected event arrived: {value:?}");
    }
}

#[derive(Clone, Debug)]
pub struct LiveFlag(Arc<AtomicBool>);

impl LiveFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_live(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    pub fn guard(&self) -> LiveGuard {
        self.0.store(true, Ordering::SeqCst);
        LiveGuard(self.0.clone())
    }
}

pub struct LiveGuard(Arc<AtomicBool>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedPathSegment {
    pub id: String,
    pub lineage: u64,
    pub generation: u64,
}

impl ObservedPathSegment {
    pub fn new(id: impl Into<String>, generation: u64) -> Self {
        Self {
            id: id.into(),
            lineage: 0,
            generation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedEvent {
    SupervisorStarted,
    SupervisorStopping,
    SupervisorStopped,
    Nested {
        id: String,
        lineage: u64,
        generation: u64,
        event: Box<Self>,
    },
    ChildStarted {
        id: String,
        generation: u64,
    },
    ChildRemoved {
        id: String,
    },
    ChildExited {
        id: String,
        generation: u64,
        status: ExitStatusView,
    },
    ChildRestartScheduled {
        id: String,
        generation: u64,
        delay: Duration,
    },
    RestartIntensityExceeded,
}

impl ObservedEvent {
    pub fn path(&self) -> Vec<ObservedPathSegment> {
        let mut path = Vec::new();
        let mut event = self;
        while let Self::Nested {
            id,
            lineage,
            generation,
            event: inner,
        } = event
        {
            path.push(ObservedPathSegment {
                id: id.clone(),
                lineage: *lineage,
                generation: *generation,
            });
            event = inner;
        }
        path
    }

    pub fn leaf(&self) -> &Self {
        match self {
            Self::Nested { event, .. } => event.leaf(),
            event => event,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventRecvError {
    Lagged(u64),
    Closed,
}

pub struct EventWatch {
    lifecycle: LifecycleWatch,
}

pub fn event_watch(handle: &SupervisorHandle) -> EventWatch {
    EventWatch {
        lifecycle: handle.subscribe_lifecycle(),
    }
}

impl EventWatch {
    pub async fn recv(&mut self) -> Result<ObservedEvent, EventRecvError> {
        loop {
            let event = self.lifecycle.next().await.ok_or(EventRecvError::Closed)?;
            match Self::convert(event) {
                Ok(Some(event)) => return Ok(event),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn convert(event: LifecycleEvent) -> Result<Option<ObservedEvent>, EventRecvError> {
        let LifecycleEvent {
            scope_path: path,
            kind,
        } = event;
        let leaf = match kind {
            LifecycleEventKind::SupervisorStarted => ObservedEvent::SupervisorStarted,
            LifecycleEventKind::SupervisorStopping => ObservedEvent::SupervisorStopping,
            LifecycleEventKind::SupervisorStopped => ObservedEvent::SupervisorStopped,
            LifecycleEventKind::Child(child) => match child.kind {
                ChildEventKind::Added => return Ok(None),
                ChildEventKind::Started { generation } => ObservedEvent::ChildStarted {
                    id: child.child_id,
                    generation,
                },
                ChildEventKind::Exited { generation, exit } => ObservedEvent::ChildExited {
                    id: child.child_id,
                    generation,
                    status: exit.into(),
                },
                ChildEventKind::Removed => ObservedEvent::ChildRemoved { id: child.child_id },
                ChildEventKind::RestartScheduled { generation, delay } => {
                    ObservedEvent::ChildRestartScheduled {
                        id: child.child_id,
                        generation,
                        delay,
                    }
                }
            },
            LifecycleEventKind::RestartIntensityExceeded { .. } => {
                ObservedEvent::RestartIntensityExceeded
            }
            LifecycleEventKind::Lagged { dropped } => {
                return Err(EventRecvError::Lagged(dropped));
            }
        };
        Ok(Some(wrap_event(leaf, &path)))
    }
}

fn wrap_event(event: ObservedEvent, path: &[crate::supervisor::ScopePathSegment]) -> ObservedEvent {
    path.iter()
        .rev()
        .fold(event, |event, segment| ObservedEvent::Nested {
            id: segment.id.clone(),
            lineage: segment.lineage,
            generation: segment.generation,
            event: Box::new(event),
        })
}

pub async fn recv_supervisor_event(events: &mut EventWatch) -> ObservedEvent {
    match timeout(EVENT_TIMEOUT, events.recv())
        .await
        .expect("timed out waiting for supervisor event")
    {
        Ok(event) => event,
        Err(EventRecvError::Lagged(skipped)) => {
            panic!("lagged while reading supervisor events: skipped {skipped}");
        }
        Err(EventRecvError::Closed) => {
            panic!("supervisor event stream closed unexpectedly");
        }
    }
}

pub async fn wait_for_child_running(
    snapshots: &mut SupervisorSnapshotReceiver,
    id: &str,
    generation: u64,
) -> ChildSnapshot {
    wait_for_snapshot(snapshots, |snapshot| {
        snapshot
            .child(id)
            .is_some_and(|child| child.generation == generation && child.state.is_running())
    })
    .await
    .child(id)
    .expect("child should exist in matching snapshot")
    .clone()
}

pub async fn wait_for_snapshot(
    snapshots: &mut SupervisorSnapshotReceiver,
    predicate: impl Fn(&SupervisorSnapshot) -> bool,
) -> SupervisorSnapshot {
    timeout(EVENT_TIMEOUT, snapshots.wait_for(predicate))
        .await
        .expect("timed out waiting for matching supervisor snapshot")
        .expect("snapshot stream closed unexpectedly")
}

pub async fn shutdown(handle: &SupervisorHandle) {
    handle.shutdown();
    wait(handle, "supervisor shutdown")
        .await
        .expect("shutdown should succeed");
}

pub async fn wait(handle: &SupervisorHandle, phase: &str) -> Result<(), SupervisorError> {
    timeout(EVENT_TIMEOUT, handle.wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
}

pub async fn shutdown_and_wait(
    handle: &SupervisorHandle,
    phase: &str,
) -> Result<(), SupervisorError> {
    timeout(EVENT_TIMEOUT, handle.shutdown_and_wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
}

pub async fn wait_started(handle: &SupervisorHandle, phase: &str) -> Result<(), SupervisorError> {
    timeout(EVENT_TIMEOUT, handle.wait_started())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
}
