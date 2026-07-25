use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    attachment::{AttachedChild, AttachedChildIdentity},
    child::{ChildSpec, OpaqueAttachment, SupervisorSpec},
    error::{ControlError, RestartMonitorError, SupervisorError},
    event::SupervisorEvent,
    monitor::{RestartMonitor, RestartWatch},
    snapshot::{ChildMembershipView, SupervisorSnapshot, SupervisorStateView},
};

type SupervisorJoinHandle = JoinHandle<Result<(), SupervisorError>>;
type DoneSender = watch::Sender<Option<Result<(), SupervisorError>>>;
type DoneReceiver = watch::Receiver<Option<Result<(), SupervisorError>>>;

#[derive(Clone)]
pub(crate) struct ControlEndpoint {
    command_tx: mpsc::Sender<SupervisorCommand>,
}

impl ControlEndpoint {
    async fn add_child(&self, child: ChildSpec) -> Result<u64, ControlError> {
        self.send(|reply| SupervisorCommand::AddChild { child, reply })
            .await
    }

    async fn remove_child(&self, id: String) -> Result<(), ControlError> {
        self.send(|reply| SupervisorCommand::RemoveChild { id, reply })
            .await
    }

    async fn add_supervisor(
        &self,
        id: String,
        supervisor: SupervisorSpec,
    ) -> Result<u64, ControlError> {
        self.send(|reply| SupervisorCommand::AddSupervisor {
            id,
            supervisor: Box::new(supervisor),
            reply,
        })
        .await
    }

    async fn send<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, ControlError>>) -> SupervisorCommand,
    ) -> Result<T, ControlError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(command(reply_tx))
            .await
            .map_err(|_| ControlError::Unavailable)?;
        reply_rx.await.map_err(|_| ControlError::Unavailable)?
    }
}

#[derive(Clone)]
struct IncarnationBinding {
    generation: u64,
    shutdown_tx: watch::Sender<bool>,
    control: ControlEndpoint,
    done_rx: DoneReceiver,
}

pub(crate) struct InitialIncarnationChannels {
    pub(crate) shutdown_tx: watch::Sender<bool>,
    pub(crate) shutdown_rx: watch::Receiver<bool>,
    pub(crate) command_tx: mpsc::Sender<SupervisorCommand>,
    pub(crate) command_rx: mpsc::Receiver<SupervisorCommand>,
    pub(crate) done_tx: DoneSender,
    pub(crate) done_rx: DoneReceiver,
}

pub(crate) type NestedChannels = Arc<Mutex<HashMap<String, Arc<StableSupervisorChannels>>>>;
pub(crate) type AttachedChildrenState = Arc<Mutex<AttachedChildrenView>>;

#[derive(Clone)]
pub(crate) struct AttachedChildrenView {
    /// `None` identifies the root supervisor. Nested views are bound to the
    /// generation of the supervisor child incarnation that owns them.
    pub(crate) generation: Option<u64>,
    pub(crate) terminal: bool,
    pub(crate) children: Vec<AttachedChildState>,
}

#[derive(Clone)]
pub(crate) struct AttachedChildState {
    pub(crate) identity: AttachedChildIdentity,
    pub(crate) attachment: Option<OpaqueAttachment>,
    pub(crate) supervisor: Option<SupervisorHandle>,
}

pub(crate) fn attached_children_state(
    generation: Option<u64>,
    children: Vec<AttachedChildState>,
) -> AttachedChildrenState {
    Arc::new(Mutex::new(AttachedChildrenView {
        generation,
        terminal: false,
        children,
    }))
}

/// Stable snapshot channel for a nested supervisor.
///
/// The sender is dropped when the supervisor child becomes terminal (it can
/// never run again), which closes the channel for watch-style consumers such
/// as [`RestartWatch`]. The retained receiver keeps serving the final snapshot
/// to [`SupervisorHandle::snapshot`] / `subscribe_snapshots` afterwards.
struct SnapshotSlot {
    tx: Option<watch::Sender<SupervisorSnapshot>>,
    rx: watch::Receiver<SupervisorSnapshot>,
}

pub(crate) struct StableSupervisorChannels {
    binding: Mutex<Option<IncarnationBinding>>,
    initial_incarnation: Mutex<Option<InitialIncarnationChannels>>,
    events_tx: broadcast::Sender<SupervisorEvent>,
    snapshots: Mutex<SnapshotSlot>,
    attached_children: AttachedChildrenState,
    nested_channels: NestedChannels,
    /// Whether these channels belong to a statically configured child (part
    /// of the parent's `SupervisorConfig`) as opposed to a dynamically added
    /// one. A replacement parent incarnation respawns exactly the static
    /// children, so reconciliation uses this to tell a reusable identity from
    /// an orphaned or colliding one.
    statically_configured: bool,
}

impl StableSupervisorChannels {
    pub(crate) fn new(
        initial_snapshot: SupervisorSnapshot,
        control_capacity: usize,
        event_capacity: usize,
        nested_channels: NestedChannels,
        attached_children: Vec<AttachedChildState>,
        statically_configured: bool,
    ) -> Arc<Self> {
        let (events_tx, _) = broadcast::channel(event_capacity);
        let (snapshots_tx, snapshots_rx) = watch::channel(initial_snapshot);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (command_tx, command_rx) = mpsc::channel(control_capacity);
        let (done_tx, done_rx) = watch::channel(None);
        Arc::new(Self {
            binding: Mutex::new(None),
            initial_incarnation: Mutex::new(Some(InitialIncarnationChannels {
                shutdown_tx,
                shutdown_rx,
                command_tx,
                command_rx,
                done_tx,
                done_rx,
            })),
            events_tx,
            snapshots: Mutex::new(SnapshotSlot {
                tx: Some(snapshots_tx),
                rx: snapshots_rx,
            }),
            attached_children: attached_children_state(Some(0), attached_children),
            nested_channels,
            statically_configured,
        })
    }

    pub(crate) fn statically_configured(&self) -> bool {
        self.statically_configured
    }

    pub(crate) fn handle(self: &Arc<Self>) -> SupervisorHandle {
        SupervisorHandle {
            kind: HandleKind::Stable(Arc::clone(self)),
        }
    }

    /// Binds a new incarnation and resets its incarnation-local snapshot.
    ///
    /// # Panics
    ///
    /// Panics if these stable channels have already been marked terminal;
    /// rebinding a terminal supervisor identity is an internal lifecycle bug.
    pub(crate) fn bind(
        self: &Arc<Self>,
        generation: u64,
        shutdown_tx: watch::Sender<bool>,
        command_tx: mpsc::Sender<SupervisorCommand>,
        done_rx: DoneReceiver,
        mut initial_snapshot: SupervisorSnapshot,
        initial_attached_children: Vec<AttachedChildState>,
    ) -> StableBindingGuard {
        let snapshots = self.snapshots();
        // The children belong to the new incarnation, but the aggregate
        // restart counter belongs to the stable supervisor identity.
        initial_snapshot.total_restarts = snapshots.borrow().total_restarts;
        snapshots.send_if_modified(|current| {
            if *current == initial_snapshot {
                return false;
            }
            *current = initial_snapshot;
            true
        });
        *self
            .attached_children
            .lock()
            .expect("attached child view poisoned") = AttachedChildrenView {
            generation: Some(generation),
            terminal: false,
            children: initial_attached_children,
        };
        *self.binding.lock().expect("stable control slot poisoned") = Some(IncarnationBinding {
            generation,
            shutdown_tx,
            control: ControlEndpoint { command_tx },
            done_rx,
        });
        StableBindingGuard {
            channels: Arc::clone(self),
            generation,
        }
    }

    fn binding(&self) -> Option<IncarnationBinding> {
        self.binding
            .lock()
            .expect("stable control slot poisoned")
            .clone()
    }

    fn initial_binding(&self) -> Option<IncarnationBinding> {
        self.initial_incarnation
            .lock()
            .expect("initial incarnation slot poisoned")
            .as_ref()
            .map(|initial| IncarnationBinding {
                generation: 0,
                shutdown_tx: initial.shutdown_tx.clone(),
                control: ControlEndpoint {
                    command_tx: initial.command_tx.clone(),
                },
                done_rx: initial.done_rx.clone(),
            })
    }

    pub(crate) fn take_initial_incarnation(&self) -> Option<InitialIncarnationChannels> {
        self.initial_incarnation
            .lock()
            .expect("initial incarnation slot poisoned")
            .take()
    }

    pub(crate) fn events(&self) -> broadcast::Sender<SupervisorEvent> {
        self.events_tx.clone()
    }

    pub(crate) fn snapshots(&self) -> watch::Sender<SupervisorSnapshot> {
        let slot = self
            .snapshots
            .lock()
            .expect("stable snapshot slot poisoned");
        slot.tx
            .as_ref()
            .expect("snapshot sender requested after stable channels became terminal")
            .clone()
    }

    pub(crate) fn snapshots_rx(&self) -> watch::Receiver<SupervisorSnapshot> {
        let slot = self
            .snapshots
            .lock()
            .expect("stable snapshot slot poisoned");
        match &slot.tx {
            Some(tx) => tx.subscribe(),
            None => slot.rx.clone(),
        }
    }

    /// Marks this supervisor child as terminal: no future incarnation will
    /// ever run. Drops the stable snapshot sender so watch-style consumers
    /// observe channel closure, and cascades to nested descendants, which can
    /// never run again either.
    ///
    /// Callers must only invoke this for judgments no ancestor reincarnation
    /// can undo: a root supervisor's decision (a non-restarted exit, or the
    /// root stopping), removal (which ends the stable identity — a later
    /// recreation mints a fresh one), or an orphaned dynamic child that no
    /// incarnation will spawn again.
    pub(crate) fn terminal(&self) {
        self.initial_incarnation
            .lock()
            .expect("initial incarnation slot poisoned")
            .take();
        {
            let mut attached_children = self
                .attached_children
                .lock()
                .expect("attached child view poisoned");
            attached_children.terminal = true;
            attached_children.children.clear();
        }
        let tx = self
            .snapshots
            .lock()
            .expect("stable snapshot slot poisoned")
            .tx
            .take();
        drop(tx);

        let descendants: Vec<_> = self
            .nested_channels
            .lock()
            .expect("nested channel map poisoned")
            .values()
            .cloned()
            .collect();
        for channels in descendants {
            channels.terminal();
        }
    }

    pub(crate) fn nested_channels(&self) -> NestedChannels {
        Arc::clone(&self.nested_channels)
    }

    pub(crate) fn attached_children(&self) -> AttachedChildrenState {
        Arc::clone(&self.attached_children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        snapshot::{ChildSnapshot, ChildStateView},
        strategy::Strategy,
    };

    #[test]
    fn binding_a_new_incarnation_resets_the_stable_snapshot() {
        let stale_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            vec![ChildSnapshot::new(
                "dynamic-worker",
                0,
                ChildStateView::Running,
            )],
        )
        .total_restarts(7);
        let initial_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            vec![ChildSnapshot::new(
                "static-worker",
                0,
                ChildStateView::Starting,
            )],
        );
        let expected_snapshot = initial_snapshot.clone().total_restarts(7);
        let channels = StableSupervisorChannels::new(
            stale_snapshot,
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
            true,
        );
        let handle = channels.handle();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);

        let _binding = channels.bind(
            2,
            shutdown_tx,
            command_tx,
            done_rx,
            initial_snapshot,
            Vec::new(),
        );

        assert_eq!(handle.snapshot(), expected_snapshot);
    }

    #[test]
    fn first_binding_does_not_publish_an_unchanged_snapshot() {
        let initial_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            vec![ChildSnapshot::new(
                "static-worker",
                0,
                ChildStateView::Starting,
            )],
        );
        let channels = StableSupervisorChannels::new(
            initial_snapshot.clone(),
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
            true,
        );
        let snapshots = channels.snapshots_rx();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);

        let _binding = channels.bind(
            0,
            shutdown_tx,
            command_tx,
            done_rx,
            initial_snapshot,
            Vec::new(),
        );

        assert!(
            !snapshots
                .has_changed()
                .expect("stable snapshot channel remains open"),
            "an unchanged first binding must not wake snapshot subscribers"
        );
    }
}

pub(crate) struct StableBindingGuard {
    channels: Arc<StableSupervisorChannels>,
    generation: u64,
}

impl Drop for StableBindingGuard {
    fn drop(&mut self) {
        let mut binding = self
            .channels
            .binding
            .lock()
            .expect("stable control slot poisoned");
        if binding
            .as_ref()
            .is_some_and(|binding| binding.generation == self.generation)
            && let Some(binding) = binding.take()
        {
            let _ = binding.shutdown_tx.send(true);
        }
    }
}

#[derive(Clone)]
struct RootHandle {
    shutdown_tx: watch::Sender<bool>,
    command_tx: mpsc::Sender<SupervisorCommand>,
    done_rx: DoneReceiver,
    events_tx: broadcast::Sender<SupervisorEvent>,
    snapshots_rx: watch::Receiver<SupervisorSnapshot>,
    attached_children: AttachedChildrenState,
    nested_channels: NestedChannels,
    join_state: Arc<Mutex<Option<(SupervisorJoinHandle, DoneSender)>>>,
}

#[derive(Clone)]
enum HandleKind {
    Root(RootHandle),
    Stable(Arc<StableSupervisorChannels>),
}

pub(crate) fn empty_nested_channels() -> NestedChannels {
    Arc::new(Mutex::new(HashMap::new()))
}

impl RootHandle {
    fn control_endpoint(&self) -> ControlEndpoint {
        ControlEndpoint {
            command_tx: self.command_tx.clone(),
        }
    }
}

pub(crate) enum SupervisorCommand {
    AddChild {
        child: ChildSpec,
        reply: oneshot::Sender<Result<u64, ControlError>>,
    },
    RemoveChild {
        id: String,
        reply: oneshot::Sender<Result<(), ControlError>>,
    },
    AddSupervisor {
        id: String,
        supervisor: Box<SupervisorSpec>,
        reply: oneshot::Sender<Result<u64, ControlError>>,
    },
}

pub(crate) struct SupervisorHandleInit {
    pub(crate) shutdown_tx: watch::Sender<bool>,
    pub(crate) command_tx: mpsc::Sender<SupervisorCommand>,
    pub(crate) nested_channels: NestedChannels,
    pub(crate) done_tx: DoneSender,
    pub(crate) done_rx: DoneReceiver,
    pub(crate) events_tx: broadcast::Sender<SupervisorEvent>,
    pub(crate) snapshots_rx: watch::Receiver<SupervisorSnapshot>,
    pub(crate) attached_children: AttachedChildrenState,
    pub(crate) join_handle: SupervisorJoinHandle,
}

/// Handle to a running supervisor, returned by [`Supervisor::spawn`](crate::Supervisor::spawn).
///
/// The handle is cheaply cloneable and can be shared across tasks. It provides:
///
/// - **Shutdown**: [`shutdown`](Self::shutdown) /
///   [`shutdown_and_wait`](Self::shutdown_and_wait).
/// - **Dynamic children**: [`add_child`](Self::add_child) /
///   [`remove_child`](Self::remove_child). Use [`supervisor`](Self::supervisor)
///   to obtain a scoped handle before changing a nested supervisor.
/// - **Observability**: [`subscribe`](Self::subscribe) for (lossy) events,
///   [`snapshot`](Self::snapshot) / [`subscribe_snapshots`](Self::subscribe_snapshots)
///   for state, [`watch_restarts`](Self::watch_restarts) for reliable restart
///   counting.
/// - **Completion**: [`wait`](Self::wait) to await the supervisor's exit.
///
/// Dropping the last handle clone requests graceful shutdown, equivalent to
/// calling [`shutdown`](Self::shutdown). Other clones keep the supervision
/// tree alive, so dropping a handle while another clone remains does not shut
/// it down. For fire-and-forget operation, keep a handle alive.
/// [`wait`](Self::wait) does not resolve until the supervisor has drained and
/// joined its child tasks.
#[derive(Clone)]
pub struct SupervisorHandle {
    kind: HandleKind,
}

impl SupervisorHandle {
    pub(crate) fn new(init: SupervisorHandleInit) -> Self {
        Self {
            kind: HandleKind::Root(RootHandle {
                shutdown_tx: init.shutdown_tx,
                command_tx: init.command_tx,
                done_rx: init.done_rx,
                events_tx: init.events_tx,
                snapshots_rx: init.snapshots_rx,
                attached_children: init.attached_children,
                nested_channels: init.nested_channels,
                join_state: Arc::new(Mutex::new(Some((init.join_handle, init.done_tx)))),
            }),
        }
    }

    /// Requests a graceful shutdown of the supervisor.
    ///
    /// This is non-blocking: it signals the supervisor to begin its shutdown
    /// sequence and returns immediately. Use [`wait`](Self::wait) or
    /// [`shutdown_and_wait`](Self::shutdown_and_wait) to await completion.
    ///
    /// Calling `shutdown` multiple times is harmless.
    pub fn shutdown(&self) {
        match &self.kind {
            HandleKind::Root(root) => {
                let _ = root.shutdown_tx.send(true);
            }
            HandleKind::Stable(stable) => {
                if let Some(binding) = stable.binding().or_else(|| stable.initial_binding()) {
                    let _ = binding.shutdown_tx.send(true);
                }
            }
        }
    }

    /// Requests a graceful shutdown and waits for the supervisor to fully stop.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.shutdown();
        self.wait().await
    }

    /// Adds a new child to the supervisor at runtime.
    ///
    /// Waits if the control channel is full. On success, returns the membership
    /// epoch assigned to the child by the supervisor. The epoch is allocated
    /// atomically with insertion, so it identifies the membership created by
    /// this specific call even if the same child id is later removed and
    /// reused. Success means the membership was inserted and its start was
    /// scheduled; under [`StartMode::Sequential`](crate::StartMode::Sequential)
    /// the child may still be queued behind an earlier readiness gate. Use
    /// [`wait_started`](Self::wait_started) when readiness is required.
    pub async fn add_child(&self, child: ChildSpec) -> Result<u64, ControlError> {
        self.control_endpoint()?.add_child(child).await
    }

    /// Adds a nested supervisor at runtime with restart-stable observation and
    /// control channels.
    ///
    /// On success, returns the membership epoch assigned atomically with the
    /// insertion. The nested handle and attachment are registered at insertion,
    /// even when [`StartMode::Sequential`](crate::StartMode::Sequential) leaves
    /// the child queued; use [`wait_started`](Self::wait_started) to await
    /// readiness.
    pub async fn add_supervisor(
        &self,
        id: impl Into<String>,
        supervisor: impl Into<SupervisorSpec>,
    ) -> Result<u64, ControlError> {
        self.control_endpoint()?
            .add_supervisor(id.into(), supervisor.into())
            .await
    }

    /// Removes a child by id from this supervisor.
    ///
    /// The child is stopped according to its [`ShutdownPolicy`](crate::ShutdownPolicy)
    /// before being removed. Removing the last child is valid; the supervisor
    /// continues idling until shutdown or until another child is added.
    pub async fn remove_child(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.control_endpoint()?.remove_child(id.into()).await
    }

    /// Returns the restart-stable handle for a direct nested supervisor.
    pub fn supervisor(&self, id: &str) -> Option<SupervisorHandle> {
        self.nested_channels()
            .lock()
            .expect("nested channel map poisoned")
            .get(id)
            .map(StableSupervisorChannels::handle)
    }

    /// Returns typed process-local attachments from this supervision tree.
    ///
    /// Direct children are returned before descendants. Every result includes
    /// the membership identity path captured from the same supervisor-owned
    /// entry as the attachment. Values with other concrete types are skipped.
    /// Attachments are not part of [`SupervisorSnapshot`] and are never
    /// serialized by the `serde` feature.
    pub fn attached_children<T>(&self) -> Vec<AttachedChild<T>>
    where
        T: Any + Send + Sync,
    {
        let mut attached = Vec::new();
        self.collect_attached_children(None, &mut Vec::new(), &mut attached);
        attached
    }

    fn collect_attached_children<T>(
        &self,
        expected_generation: Option<u64>,
        parent_path: &mut Vec<AttachedChildIdentity>,
        attached: &mut Vec<AttachedChild<T>>,
    ) where
        T: Any + Send + Sync,
    {
        let view = self
            .attached_children_state()
            .lock()
            .expect("attached child view poisoned")
            .clone();
        if view.terminal
            || expected_generation.is_some_and(|generation| view.generation != Some(generation))
        {
            return;
        }
        let children = view.children;

        for child in &children {
            let Some(value) = child
                .attachment
                .as_ref()
                .and_then(|value| Arc::clone(value).downcast::<T>().ok())
            else {
                continue;
            };
            let mut path = parent_path.clone();
            path.push(child.identity.clone());
            attached.push(AttachedChild::new(path, value, child.supervisor.clone()));
        }

        for child in children {
            let Some(supervisor) = child.supervisor else {
                continue;
            };
            let generation = child.identity.generation;
            parent_path.push(child.identity);
            supervisor.collect_attached_children(Some(generation), parent_path, attached);
            parent_path.pop();
        }
    }

    /// Waits for the supervisor to stop.
    ///
    /// The first caller to `wait` joins the underlying Tokio task. Subsequent
    /// callers (including concurrent ones from cloned handles) receive the
    /// same result via a shared watch channel. A successful return means the
    /// runtime has finished draining and joining supervised child tasks.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        let (mut done_rx, join) = match &self.kind {
            HandleKind::Root(root) => {
                if let Some(result) = root.done_rx.borrow().clone() {
                    return result;
                }
                let join = root
                    .join_state
                    .lock()
                    .expect("join_state mutex poisoned")
                    .take();
                (root.done_rx.clone(), join)
            }
            HandleKind::Stable(stable) => {
                let binding = stable
                    .binding()
                    .or_else(|| stable.initial_binding())
                    .ok_or_else(|| {
                        SupervisorError::Internal(
                            "nested supervisor incarnation is unavailable".to_owned(),
                        )
                    })?;
                (binding.done_rx, None)
            }
        };

        if let Some((join_handle, done_tx)) = join {
            let result = match join_handle.await {
                Ok(result) => result,
                Err(err) => Err(SupervisorError::Internal(format!(
                    "supervisor task failed to join: {err}"
                ))),
            };
            let _ = done_tx.send(Some(result.clone()));
            return result;
        }

        if let Some(result) = done_rx.borrow().clone() {
            return result;
        }
        done_rx
            .wait_for(|value| value.is_some())
            .await
            .map_err(|_| {
                SupervisorError::Internal("supervisor completion channel closed".to_owned())
            })?;

        done_rx.borrow().clone().unwrap_or_else(|| {
            Err(SupervisorError::Internal(
                "missing supervisor completion result".to_owned(),
            ))
        })
    }

    /// Waits until every current child generation has completed startup.
    ///
    /// Explicitly gated children complete startup when they call
    /// [`ChildContext::mark_ready`](crate::ChildContext::mark_ready); ordinary
    /// children are ready as soon as they are spawned. Readiness remains
    /// latched after a child exits, and resets when that child restarts. Nested
    /// supervisors report ready only after their own children are ready. An
    /// empty supervisor is ready immediately.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorError::StartupAborted`] if a gated child exits or
    /// the supervisor stops before readiness is reported.
    pub async fn wait_started(&self) -> Result<(), SupervisorError> {
        let mut snapshots = self.snapshots_rx();
        loop {
            let snapshot = snapshots.borrow_and_update().clone();
            if snapshot
                .children
                .iter()
                .filter(|child| child.membership == ChildMembershipView::Active)
                .all(|child| child.started)
            {
                return Ok(());
            }
            if let Some(child) = snapshot.children.iter().find(|child| {
                child.membership == ChildMembershipView::Active
                    && !child.started
                    && child.startup_aborted
            }) {
                return Err(SupervisorError::StartupAborted(format!(
                    "child `{}` exited before reporting readiness",
                    child.id
                )));
            }
            if snapshot.state == SupervisorStateView::Stopped {
                self.wait().await?;
                return Err(SupervisorError::StartupAborted(
                    "supervisor stopped before all children reported readiness".to_owned(),
                ));
            }
            snapshots.changed().await.map_err(|_| {
                SupervisorError::StartupAborted(
                    "supervisor stopped before all children reported readiness".to_owned(),
                )
            })?;
        }
    }

    /// Captures a direct child's current generation and returns an awaitable
    /// that resolves once the child is next observed running with a greater
    /// generation.
    ///
    /// Call this before triggering the crash: the baseline generation is
    /// captured here, synchronously, not when the monitor is awaited. A monitor
    /// created after a restart has fully completed waits for the next restart.
    ///
    /// This observes only direct children of this supervisor. Children inside
    /// nested supervisors are not visible by id at this level; a path-aware
    /// monitor is a future extension.
    pub fn monitor_restart(
        &self,
        id: impl Into<String>,
    ) -> Result<RestartMonitor, RestartMonitorError> {
        let id = id.into();
        let snapshots_rx = self.snapshots_rx();
        let snapshot = snapshots_rx.borrow();
        let child = snapshot
            .child(&id)
            .ok_or_else(|| RestartMonitorError::UnknownChild(id.clone()))?;
        if child.membership == ChildMembershipView::Removing {
            return Err(RestartMonitorError::ChildRemoved(id));
        }
        let generation = child.generation;
        drop(snapshot);

        Ok(RestartMonitor::new(id, generation, snapshots_rx))
    }

    /// Returns a new receiver for supervisor lifecycle events.
    ///
    /// # Events are lossy observability, not durable control
    ///
    /// The receiver is backed by a bounded broadcast channel. If the receiver
    /// falls behind by more than the configured
    /// [`event_channel_capacity`](crate::SupervisorBuilder::event_channel_capacity),
    /// it receives a `Lagged` error and skips the missed events. Events
    /// forwarded from nested supervisors cross an additional bounded internal
    /// channel and can be dropped there without a `Lagged` marker on this
    /// receiver.
    ///
    /// This contract makes the event stream suitable for logging, tracing,
    /// and dashboards, but **not** for driving safety or control logic: a
    /// consumer that counts events can silently under-count. Consumers that
    /// nevertheless gate decisions on events must fail closed on `Lagged`
    /// (treat the gap as if the guarded condition occurred), or better, use a
    /// cumulative source that cannot miss occurrences:
    ///
    /// - [`watch_restarts`](Self::watch_restarts) for restart activity, or
    /// - [`subscribe_snapshots`](Self::subscribe_snapshots) with the
    ///   monotonic [`SupervisorSnapshot::total_restarts`] and per-child
    ///   [`ChildSnapshot::restart_count`](crate::ChildSnapshot::restart_count)
    ///   counters. Snapshots are delivered over a `watch` channel, which
    ///   conflates intermediate values but never lags, so counter deltas
    ///   account for every restart.
    pub fn subscribe(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.events_tx().subscribe()
    }

    /// Returns a [`RestartWatch`] that reliably reports this supervisor's
    /// restart activity as it happens.
    ///
    /// The watch observes the monotonic
    /// [`SupervisorSnapshot::total_restarts`] counter over the lossless
    /// snapshot `watch` channel, so unlike counting [`subscribe`](Self::subscribe)
    /// events it can never silently miss a restart the counter covers —
    /// conflated updates are reported as one delta covering every restart in
    /// the batch. Use it for control logic such as aggregate restart
    /// breakers.
    ///
    /// The counter covers this supervisor's **direct children** only. To
    /// observe a nested subtree, watch each nested supervisor's own handle
    /// ([`supervisor`](Self::supervisor)) — `total_restarts` does not
    /// aggregate across depth, and under group strategies sibling respawns
    /// are not counted. See [`SupervisorSnapshot::total_restarts`] for the
    /// exact contract.
    ///
    /// The baseline is captured here, synchronously: only restarts recorded
    /// after this call are reported.
    ///
    /// ```no_run
    /// # async fn example(handle: tokio_supervisor::SupervisorHandle) {
    /// let mut restarts = handle.watch_restarts();
    /// while let Some(newly_recorded) = restarts.next().await {
    ///     // Feed `newly_recorded` into a sliding-window breaker.
    /// }
    /// # }
    /// ```
    pub fn watch_restarts(&self) -> RestartWatch {
        RestartWatch::new(self.snapshots_rx())
    }

    /// Returns a clone of the latest [`SupervisorSnapshot`].
    pub fn snapshot(&self) -> SupervisorSnapshot {
        self.snapshots_rx().borrow().clone()
    }

    /// Returns a watch receiver that is updated each time the supervisor's
    /// snapshot changes. Useful for polling or `wait_for`-style patterns.
    ///
    /// # Waiting until all children are running
    ///
    /// ```no_run
    /// use tokio_supervisor::{ChildSpec, ChildStateView, SupervisorBuilder};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let supervisor = SupervisorBuilder::new()
    ///     .child(ChildSpec::new("worker", |ctx| async move {
    ///         ctx.shutdown_token().cancelled().await;
    ///         Ok(())
    ///     }))
    ///     .build()?;
    ///
    /// let handle = supervisor.spawn();
    /// handle
    ///     .subscribe_snapshots()
    ///     .wait_for(|snapshot| {
    ///         snapshot
    ///             .children
    ///             .iter()
    ///             .all(|child| child.state == ChildStateView::Running)
    ///     })
    ///     .await?;
    /// # handle.shutdown();
    /// # handle.wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe_snapshots(&self) -> watch::Receiver<SupervisorSnapshot> {
        self.snapshots_rx()
    }

    fn control_endpoint(&self) -> Result<ControlEndpoint, ControlError> {
        match &self.kind {
            HandleKind::Root(root) => Ok(root.control_endpoint()),
            HandleKind::Stable(stable) => stable
                .binding()
                .or_else(|| stable.initial_binding())
                .map(|binding| binding.control)
                .ok_or(ControlError::Unavailable),
        }
    }

    fn events_tx(&self) -> broadcast::Sender<SupervisorEvent> {
        match &self.kind {
            HandleKind::Root(root) => root.events_tx.clone(),
            HandleKind::Stable(stable) => stable.events(),
        }
    }

    fn snapshots_rx(&self) -> watch::Receiver<SupervisorSnapshot> {
        match &self.kind {
            HandleKind::Root(root) => root.snapshots_rx.clone(),
            HandleKind::Stable(stable) => stable.snapshots_rx(),
        }
    }

    fn nested_channels(&self) -> NestedChannels {
        match &self.kind {
            HandleKind::Root(root) => Arc::clone(&root.nested_channels),
            HandleKind::Stable(stable) => stable.nested_channels(),
        }
    }

    fn attached_children_state(&self) -> AttachedChildrenState {
        match &self.kind {
            HandleKind::Root(root) => Arc::clone(&root.attached_children),
            HandleKind::Stable(stable) => stable.attached_children(),
        }
    }
}
