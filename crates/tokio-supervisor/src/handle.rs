use std::{
    any::Any,
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, Ordering},
    },
};

use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    attachment::{AttachedChild, AttachedChildIdentity},
    child::{ChildSpec, OpaqueAttachment, SupervisorSpec},
    error::{ControlError, SupervisorError},
    event::SupervisorEvent,
    lifecycle::{LifecycleHub, LifecycleWatch},
    snapshot::{
        ChildMembershipView, ChildSnapshot, ChildStateView, SupervisorSnapshot, SupervisorStateView,
    },
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

    async fn add_supervisor(&self, supervisor: PendingSupervisorSpec) -> Result<u64, ControlError> {
        self.send(|reply| SupervisorCommand::AddSupervisor { supervisor, reply })
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
/// as [`LifecycleWatch`]. The retained receiver keeps
/// serving the final snapshot to [`SupervisorHandle::snapshot`] /
/// `subscribe_snapshots` afterwards.
struct SnapshotSlot {
    tx: Option<watch::Sender<SupervisorSnapshot>>,
    rx: watch::Receiver<SupervisorSnapshot>,
}

/// Stable event channel for a supervisor identity.
///
/// Mirrors [`SnapshotSlot`]: the sender is dropped when the identity becomes
/// terminal so watch-style consumers observe closure, and the retained
/// receiver keeps `subscribe` working afterwards by resubscribing from it.
///
/// The retained receiver is also the reason
/// [`StableSupervisorChannels::reset_declared_capacities`] can detect whether
/// anyone outside these channels is listening: a `receiver_count` of exactly
/// one means only `rx` is attached, so the channel can be replaced to honour a
/// new capacity without orphaning a subscriber.
struct EventSlot {
    tx: Option<broadcast::Sender<SupervisorEvent>>,
    rx: broadcast::Receiver<SupervisorEvent>,
}

struct StableBindingState {
    current: Option<IncarnationBinding>,
    /// Whether any incarnation has ever bound to this identity. Distinguishes
    /// a scope that has not started yet from one resting between
    /// incarnations, which [`SupervisorHandle::wait`] treats differently.
    bound_once: bool,
    terminal: bool,
}

enum StartupSnapshot {
    Bound(SupervisorSnapshot),
    Unbound,
    Terminal,
}

/// What [`SupervisorHandle::wait`] should do for a non-root identity.
enum WaitTarget {
    /// An incarnation is bound; await its completion channel.
    Bound(DoneReceiver),
    /// Nothing has ever bound, so the scope has not started yet. The caller
    /// waits for a first incarnation rather than reporting a failure.
    NeverBound,
    /// A previous incarnation ended and no replacement has bound yet.
    BetweenIncarnations,
    /// No incarnation can ever run again.
    Terminal,
}

pub(crate) struct PendingSupervisorSpec {
    supervisor: Option<Box<SupervisorSpec>>,
}

impl PendingSupervisorSpec {
    fn new(supervisor: SupervisorSpec) -> Self {
        Self {
            supervisor: Some(Box::new(supervisor)),
        }
    }

    pub(crate) fn spec_mut(&mut self) -> &mut SupervisorSpec {
        self.supervisor
            .as_deref_mut()
            .expect("pending supervisor spec was already accepted")
    }

    pub(crate) fn accept(mut self) -> SupervisorSpec {
        *self
            .supervisor
            .take()
            .expect("pending supervisor spec was already accepted")
    }
}

impl Drop for PendingSupervisorSpec {
    fn drop(&mut self) {
        if let Some(supervisor) = &self.supervisor {
            supervisor.supervisor.channels.terminal();
        }
    }
}

#[derive(Clone)]
enum RootExtraSlot {
    NotRoot,
    Pending,
    Ready(Arc<RootExtra>),
}

pub(crate) struct StableSupervisorChannels {
    binding: Mutex<StableBindingState>,
    binding_revision: watch::Sender<u64>,
    initial_incarnation: Mutex<Option<InitialIncarnationChannels>>,
    events: Mutex<EventSlot>,
    lifecycle: Arc<LifecycleHub>,
    snapshots: Mutex<SnapshotSlot>,
    attached_children: AttachedChildrenState,
    nested_channels: NestedChannels,
    /// Which kind of parent edge, if any, has claimed this identity.
    ///
    /// Holds one of [`EDGE_UNCLAIMED`], [`EDGE_DYNAMIC`], or [`EDGE_STATIC`].
    /// A reserved identity starts unclaimed and is claimed exactly once, when
    /// it is spawned as a root or attached to a parent. A replacement parent
    /// incarnation respawns exactly the static children, so reconciliation
    /// uses the static/dynamic distinction to tell a reusable identity from an
    /// orphaned or colliding one.
    edge_kind: AtomicU8,
    root_extra: Mutex<RootExtraSlot>,
    handle_lease: Mutex<Weak<HandleLease>>,
}

/// No parent edge has claimed this identity yet.
const EDGE_UNCLAIMED: u8 = 0;
/// Claimed by a dynamic edge: a runtime insertion, or a spawned root.
const EDGE_DYNAMIC: u8 = 1;
/// Claimed by a static edge: declared in the parent's `SupervisorConfig`.
const EDGE_STATIC: u8 = 2;

impl StableSupervisorChannels {
    pub(crate) fn new(
        initial_snapshot: SupervisorSnapshot,
        control_capacity: usize,
        event_capacity: usize,
        nested_channels: NestedChannels,
        attached_children: Vec<AttachedChildState>,
    ) -> Arc<Self> {
        let (events_tx, events_rx) = broadcast::channel(event_capacity);
        let (snapshots_tx, snapshots_rx) = watch::channel(initial_snapshot);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (command_tx, command_rx) = mpsc::channel(control_capacity.max(1));
        let (done_tx, done_rx) = watch::channel(None);
        let (binding_revision, _) = watch::channel(0);
        Arc::new(Self {
            binding: Mutex::new(StableBindingState {
                current: None,
                bound_once: false,
                terminal: false,
            }),
            binding_revision,
            initial_incarnation: Mutex::new(Some(InitialIncarnationChannels {
                shutdown_tx,
                shutdown_rx,
                command_tx,
                command_rx,
                done_tx,
                done_rx,
            })),
            events: Mutex::new(EventSlot {
                tx: Some(events_tx),
                rx: events_rx,
            }),
            lifecycle: LifecycleHub::new(),
            snapshots: Mutex::new(SnapshotSlot {
                tx: Some(snapshots_tx),
                rx: snapshots_rx,
            }),
            attached_children: attached_children_state(Some(0), attached_children),
            nested_channels,
            edge_kind: AtomicU8::new(EDGE_UNCLAIMED),
            root_extra: Mutex::new(RootExtraSlot::NotRoot),
            handle_lease: Mutex::new(Weak::new()),
        })
    }

    pub(crate) fn statically_configured(&self) -> bool {
        self.edge_kind.load(Ordering::Acquire) == EDGE_STATIC
    }

    pub(crate) fn claim_edge(&self, statically_configured: bool) {
        let edge_kind = if statically_configured {
            EDGE_STATIC
        } else {
            EDGE_DYNAMIC
        };
        self.edge_kind.store(edge_kind, Ordering::Release);
    }

    fn assert_reconfigurable(&self) {
        let binding = self.binding.lock().expect("stable control slot poisoned");
        assert!(
            binding.current.is_none() && !binding.terminal,
            "a bound or terminal supervisor identity cannot be reconfigured"
        );
    }

    /// Republishes the declared view — snapshot, nested identities, and
    /// attachments — for a reserved identity whose configuration changed.
    ///
    /// Builder mutators call this on every change, so it deliberately touches
    /// no channel: reallocating the control, event, and completion channels
    /// per mutation would make building an `n`-child scope allocate `n` full
    /// channel sets. Capacities are applied once, by
    /// [`reset_declared_capacities`](Self::reset_declared_capacities).
    pub(crate) fn reset_declared_view(
        &self,
        initial_snapshot: SupervisorSnapshot,
        nested_channels: NestedChannels,
        attached_children: Vec<AttachedChildState>,
    ) {
        self.assert_reconfigurable();

        self.snapshots().send_if_modified(|snapshot| {
            if *snapshot == initial_snapshot {
                false
            } else {
                *snapshot = initial_snapshot;
                true
            }
        });
        *self
            .nested_channels
            .lock()
            .expect("nested channel map poisoned") = nested_channels
            .lock()
            .expect("replacement nested channel map poisoned")
            .clone();
        *self
            .attached_children
            .lock()
            .expect("attached child view poisoned") = AttachedChildrenView {
            generation: Some(0),
            terminal: false,
            children: attached_children,
        };
    }

    /// Sizes this identity's channels from the finished configuration.
    ///
    /// Called once, when a reserved identity is built, because capacities are
    /// only observable from the first incarnation onwards.
    ///
    /// The event channel is replaced only when the retained [`EventSlot::rx`]
    /// is its sole receiver. A subscriber created from a pre-build handle
    /// therefore pins the capacity in force when it subscribed: replacing the
    /// channel under it would leave it permanently closed rather than merely
    /// resized. [`event_channel_capacity`](crate::SupervisorBuilder::event_channel_capacity)
    /// documents that ordering requirement.
    pub(crate) fn reset_declared_capacities(&self, control_capacity: usize, event_capacity: usize) {
        self.assert_reconfigurable();

        let mut events = self.events.lock().expect("stable event slot poisoned");
        if events
            .tx
            .as_ref()
            .is_some_and(|events_tx| events_tx.receiver_count() == 1)
        {
            let (events_tx, events_rx) = broadcast::channel(event_capacity.max(1));
            *events = EventSlot {
                tx: Some(events_tx),
                rx: events_rx,
            };
        }
        drop(events);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (command_tx, command_rx) = mpsc::channel(control_capacity.max(1));
        let (done_tx, done_rx) = watch::channel(None);
        *self
            .initial_incarnation
            .lock()
            .expect("initial incarnation slot poisoned") = Some(InitialIncarnationChannels {
            shutdown_tx,
            shutdown_rx,
            command_tx,
            command_rx,
            done_tx,
            done_rx,
        });
    }

    pub(crate) fn project_declared_children(&self, ids: Vec<String>) {
        // Projected epochs are positional, and `bind` later overwrites them
        // with epochs minted from this hub. The two agree — which is what
        // makes the documented `(child_id, membership_epoch)` upsert on
        // `watch_lifecycle` idempotent across the pre-spawn baseline — only
        // because a reserved identity has not minted any epoch yet, so the
        // hub allocates 0, 1, 2, ... in the same declaration order.
        debug_assert_eq!(
            self.lifecycle.peek_membership_epoch(),
            0,
            "declared projection assumes an identity that has not minted membership epochs yet"
        );
        let snapshots = self.snapshots();
        snapshots.send_if_modified(|snapshot| {
            let children = ids
                .into_iter()
                .enumerate()
                .map(|(membership_epoch, id)| ChildSnapshot {
                    id,
                    membership_epoch: membership_epoch as u64,
                    generation: 0,
                    started: false,
                    startup_aborted: false,
                    state: ChildStateView::Starting,
                    membership: ChildMembershipView::Active,
                    last_exit: None,
                    last_exit_cancelled: false,
                    restart_count: 0,
                    next_restart_in: None,
                    supervisor: None,
                })
                .collect::<Vec<_>>();
            if snapshot.children == children {
                false
            } else {
                snapshot.children = children;
                true
            }
        });
    }

    pub(crate) fn handle(self: &Arc<Self>) -> SupervisorHandle {
        let lease = {
            let mut slot = self
                .handle_lease
                .lock()
                .expect("handle lease slot poisoned");
            slot.upgrade().unwrap_or_else(|| {
                let lease = Arc::new(HandleLease {
                    channels: Arc::downgrade(self),
                });
                *slot = Arc::downgrade(&lease);
                lease
            })
        };
        SupervisorHandle {
            channels: Arc::clone(self),
            _lease: Some(lease),
        }
    }

    pub(crate) fn internal_handle(self: &Arc<Self>) -> SupervisorHandle {
        SupervisorHandle {
            channels: Arc::clone(self),
            _lease: None,
        }
    }

    pub(crate) fn install_root_extra(&self, root_extra: RootExtra) {
        let mut slot = self.root_extra.lock().expect("root extra slot poisoned");
        assert!(
            matches!(*slot, RootExtraSlot::Pending),
            "root extra must be installed exactly once after root classification"
        );
        *slot = RootExtraSlot::Ready(Arc::new(root_extra));
        drop(slot);
        self.bump_binding_revision();
    }

    fn root_extra(&self) -> RootExtraSlot {
        self.root_extra
            .lock()
            .expect("root extra slot poisoned")
            .clone()
    }

    /// Binds a new incarnation and resets its incarnation-local snapshot.
    ///
    /// Returns `None` if these stable channels were marked terminal before the
    /// incarnation could bind. This can race with a parent incarnation ending
    /// while a nested supervisor is starting; the terminal judgment wins.
    pub(crate) fn bind(
        self: &Arc<Self>,
        generation: u64,
        shutdown_tx: watch::Sender<bool>,
        command_tx: mpsc::Sender<SupervisorCommand>,
        done_rx: DoneReceiver,
        mut initial_snapshot: SupervisorSnapshot,
        mut initial_attached_children: Vec<AttachedChildState>,
    ) -> Option<BoundIncarnation> {
        let mut binding = self.binding.lock().expect("stable control slot poisoned");
        if binding.terminal {
            return None;
        }

        // Keep terminalization excluded through snapshot/attachment reset and
        // publication of the new control binding. Every sender needed by the
        // incarnation is acquired inside the same lifecycle boundary, so
        // `run_as_child` never has to reopen the terminalization race after
        // binding.
        let snapshots = self.snapshots();
        let events = self.events();
        let lifecycle = self.lifecycle();
        // Membership epochs belong to the stable supervisor identity, not an
        // individual incarnation. Assign the new static memberships before
        // publishing the incarnation baseline so snapshot reducers and later
        // `Added` events agree on their keys.
        for child in &mut initial_snapshot.children {
            let membership_epoch = lifecycle.next_membership_epoch();
            child.membership_epoch = membership_epoch;
            if let Some(attached) = initial_attached_children
                .iter_mut()
                .find(|attached| attached.identity.id == child.id)
            {
                attached.identity.membership_epoch = membership_epoch;
            }
        }
        // The children belong to the new incarnation, but the aggregate
        // restart counter belongs to the stable supervisor identity.
        initial_snapshot.total_restarts = snapshots.borrow().total_restarts;
        initial_snapshot.lifecycle_seq = lifecycle.seq();
        snapshots.send_if_modified(|current| {
            if *current == initial_snapshot {
                return false;
            }
            *current = initial_snapshot;
            true
        });
        let attachment_generation = match *self.root_extra.lock().expect("root extra slot poisoned")
        {
            RootExtraSlot::NotRoot => Some(generation),
            RootExtraSlot::Pending | RootExtraSlot::Ready(_) => None,
        };
        *self
            .attached_children
            .lock()
            .expect("attached child view poisoned") = AttachedChildrenView {
            generation: attachment_generation,
            terminal: false,
            children: initial_attached_children,
        };
        binding.current = Some(IncarnationBinding {
            generation,
            shutdown_tx,
            control: ControlEndpoint { command_tx },
            done_rx,
        });
        binding.bound_once = true;
        self.bump_binding_revision();
        Some(BoundIncarnation {
            guard: StableBindingGuard {
                channels: Arc::clone(self),
                generation,
            },
            snapshots,
            events,
            lifecycle,
        })
    }

    fn current_binding(&self) -> Option<IncarnationBinding> {
        let binding = self.binding.lock().expect("stable control slot poisoned");
        if binding.terminal {
            return None;
        }
        binding.current.clone()
    }

    /// Classifies what a non-root [`SupervisorHandle::wait`] should do, at one
    /// binding-serialized point.
    fn wait_target(&self) -> WaitTarget {
        let binding = self.binding.lock().expect("stable control slot poisoned");
        if binding.terminal {
            WaitTarget::Terminal
        } else if let Some(current) = binding.current.as_ref() {
            WaitTarget::Bound(current.done_rx.clone())
        } else if binding.bound_once {
            WaitTarget::BetweenIncarnations
        } else {
            WaitTarget::NeverBound
        }
    }

    /// Reads binding presence and its incarnation-local snapshot at one
    /// binding-serialized point.
    ///
    /// `bind` resets the snapshot before publishing `current` while holding
    /// this same mutex. Keeping the receiver read inside that boundary avoids
    /// pairing a previous incarnation's ready snapshot with a newly published
    /// binding.
    fn startup_snapshot(
        &self,
        snapshots: &mut watch::Receiver<SupervisorSnapshot>,
    ) -> StartupSnapshot {
        let binding = self.binding.lock().expect("stable control slot poisoned");
        if binding.terminal {
            StartupSnapshot::Terminal
        } else if binding.current.is_none() {
            StartupSnapshot::Unbound
        } else {
            StartupSnapshot::Bound(snapshots.borrow_and_update().clone())
        }
    }

    pub(crate) fn take_initial_incarnation(
        &self,
        generation: u64,
    ) -> Option<InitialIncarnationChannels> {
        if generation != 0 {
            return None;
        }

        // Keep extraction and terminality ordered, but do not publish the
        // control endpoint yet. `bind` is the point at which an incarnation
        // becomes visible; publishing here would let an empty scope appear
        // started during the synchronous handoff into `bind`.
        let binding = self.binding.lock().expect("stable control slot poisoned");
        if binding.terminal {
            return None;
        }
        let mut initial = self
            .initial_incarnation
            .lock()
            .expect("initial incarnation slot poisoned");
        let channels = initial.take()?;
        drop(binding);
        Some(channels)
    }

    fn bump_binding_revision(&self) {
        self.binding_revision.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }

    fn binding_revision_rx(&self) -> watch::Receiver<u64> {
        self.binding_revision.subscribe()
    }

    pub(crate) fn events(&self) -> broadcast::Sender<SupervisorEvent> {
        self.events
            .lock()
            .expect("stable event slot poisoned")
            .tx
            .as_ref()
            .expect("event sender requested after stable channels became terminal")
            .clone()
    }

    pub(crate) fn events_rx(&self) -> broadcast::Receiver<SupervisorEvent> {
        let slot = self.events.lock().expect("stable event slot poisoned");
        match &slot.tx {
            Some(tx) => tx.subscribe(),
            None => slot.rx.resubscribe(),
        }
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

    pub(crate) fn lifecycle(&self) -> Arc<LifecycleHub> {
        Arc::clone(&self.lifecycle)
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
        let active = {
            let mut binding = self.binding.lock().expect("stable control slot poisoned");
            binding.terminal = true;
            binding.current.take()
        };
        if let Some(active) = active {
            let _ = active.shutdown_tx.send(true);
        }
        self.bump_binding_revision();
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
        let events_tx = self
            .events
            .lock()
            .expect("stable event slot poisoned")
            .tx
            .take();
        drop(events_tx);
        self.lifecycle.terminal();

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

    pub(crate) fn mark_root(&self) {
        let mut root_extra = self.root_extra.lock().expect("root extra slot poisoned");
        assert!(
            matches!(*root_extra, RootExtraSlot::NotRoot),
            "supervisor identity was classified as root more than once"
        );
        *root_extra = RootExtraSlot::Pending;
        drop(root_extra);
        self.attached_children
            .lock()
            .expect("attached child view poisoned")
            .generation = None;
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
        );
        let handle = channels.handle();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);

        let _bound = channels
            .bind(
                2,
                shutdown_tx,
                command_tx,
                done_rx,
                initial_snapshot,
                Vec::new(),
            )
            .expect("live stable channels bind");

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
        );
        let snapshots = channels.snapshots_rx();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);

        let _bound = channels
            .bind(
                0,
                shutdown_tx,
                command_tx,
                done_rx,
                initial_snapshot,
                Vec::new(),
            )
            .expect("live stable channels bind");

        assert!(
            !snapshots
                .has_changed()
                .expect("stable snapshot channel remains open"),
            "an unchanged first binding must not wake snapshot subscribers"
        );
    }

    #[tokio::test]
    async fn initial_channel_handoff_stays_unavailable_and_not_ready_until_bind() {
        use std::time::Duration;

        let channels = StableSupervisorChannels::new(
            SupervisorSnapshot::new(
                SupervisorStateView::Running,
                Strategy::OneForOne,
                Vec::new(),
            ),
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
        );
        let handle = channels.handle();

        let initial = channels
            .take_initial_incarnation(0)
            .expect("initial incarnation channels are available");

        assert!(matches!(
            handle.control_endpoint(),
            Err(ControlError::Unavailable)
        ));
        let mut started = Box::pin(handle.wait_started());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut started)
                .await
                .is_err(),
            "an empty scope must not be ready during initial channel handoff"
        );

        let _bound = channels
            .bind(
                0,
                initial.shutdown_tx,
                initial.command_tx,
                initial.done_rx,
                handle.snapshot(),
                Vec::new(),
            )
            .expect("initial incarnation binds");
        tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("bound empty scope becomes ready")
            .expect("bound empty scope starts");
    }

    #[test]
    fn startup_snapshot_does_not_pair_a_stale_ready_view_with_a_new_binding() {
        let ready_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            Vec::new(),
        );
        let channels = StableSupervisorChannels::new(
            ready_snapshot.clone(),
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
        );
        let mut snapshots = channels.snapshots_rx();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);
        let first = channels
            .bind(
                0,
                shutdown_tx,
                command_tx,
                done_rx,
                ready_snapshot,
                Vec::new(),
            )
            .expect("first incarnation binds");

        let stale_ready = snapshots.borrow_and_update().clone();
        assert!(stale_ready.children.is_empty());
        drop(first);

        let starting_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            vec![ChildSnapshot::new(
                "gated-worker",
                0,
                ChildStateView::Starting,
            )],
        );
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);
        let _second = channels
            .bind(
                1,
                shutdown_tx,
                command_tx,
                done_rx,
                starting_snapshot,
                Vec::new(),
            )
            .expect("replacement incarnation binds");

        let StartupSnapshot::Bound(observed) = channels.startup_snapshot(&mut snapshots) else {
            panic!("replacement binding must be observable");
        };
        assert_eq!(observed.children.len(), 1);
        assert!(!observed.children[0].started);
    }

    #[tokio::test]
    async fn root_wait_during_extra_handoff_claims_the_join_handle() {
        use std::time::Duration;

        let snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            Vec::new(),
        );
        let channels = StableSupervisorChannels::new(
            snapshot.clone(),
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
        );
        let handle = channels.handle();
        channels.mark_root();
        let initial = channels
            .take_initial_incarnation(0)
            .expect("initial incarnation channels are available");
        let root_done_tx = initial.done_tx.clone();
        let root_done_rx = initial.done_rx.clone();
        let _bound = channels
            .bind(
                0,
                initial.shutdown_tx,
                initial.command_tx,
                initial.done_rx,
                snapshot,
                Vec::new(),
            )
            .expect("root incarnation binds");

        let mut waiting = Box::pin(handle.wait());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err(),
            "wait must not fall through to the nested path while root extra is pending"
        );

        let join = tokio::spawn(async { Ok(()) });
        channels.install_root_extra(RootExtra::new(root_done_rx, join, root_done_tx));
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("wait observes root-extra publication")
            .expect("wait joins the root task");
    }

    #[tokio::test]
    async fn cancelled_add_terminalizes_when_the_queued_supervisor_is_dropped() {
        let child = crate::DynamicSupervisorBuilder::new();
        let retained = child.handle();
        let child = child.build().expect("nested supervisor builds");
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let endpoint = ControlEndpoint { command_tx };
        let mut adding = Box::pin(endpoint.add_supervisor(PendingSupervisorSpec::new(
            SupervisorSpec::new("nested", child),
        )));

        let queued = tokio::select! {
            command = command_rx.recv() => command.expect("add command queued"),
            result = &mut adding => panic!("add unexpectedly completed: {result:?}"),
        };
        drop(adding);
        drop(queued);

        assert!(
            retained.subscribe_snapshots().changed().await.is_err(),
            "dropping a queued add after caller cancellation terminalizes its reserved identity"
        );
    }

    #[test]
    fn a_superseded_handle_lease_does_not_shut_the_root_down() {
        let snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            Vec::new(),
        );
        let channels =
            StableSupervisorChannels::new(snapshot.clone(), 8, 8, empty_nested_channels(), vec![]);
        let released = channels.handle();
        channels.mark_root();
        let initial = channels
            .take_initial_incarnation(0)
            .expect("initial incarnation channels are available");
        let mut shutdown_rx = initial.shutdown_rx.clone();
        let _bound = channels
            .bind(
                0,
                initial.shutdown_tx,
                initial.command_tx,
                initial.done_rx,
                snapshot,
                Vec::new(),
            )
            .expect("root incarnation binds");

        // Stand in for the replacement lease that `handle()` installs once the
        // released lease's strong count reaches zero but before its `Drop`
        // body runs.
        let replacement = Arc::new(HandleLease {
            channels: Arc::downgrade(&channels),
        });
        *channels
            .handle_lease
            .lock()
            .expect("handle lease slot poisoned") = Arc::downgrade(&replacement);

        drop(released);

        assert!(
            !*shutdown_rx.borrow_and_update(),
            "a lease superseded by a live replacement must not signal shutdown"
        );

        drop(replacement);
        assert!(
            *shutdown_rx.borrow_and_update(),
            "releasing the live replacement still shuts the root down"
        );
    }

    #[test]
    fn terminalization_after_bind_uses_the_atomically_acquired_event_sender() {
        let initial_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            Vec::new(),
        );
        let channels = StableSupervisorChannels::new(
            initial_snapshot.clone(),
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
        );
        let mut events = channels.events_rx();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (_done_tx, done_rx) = watch::channel(None);

        let bound = channels
            .bind(
                0,
                shutdown_tx,
                command_tx,
                done_rx,
                initial_snapshot,
                Vec::new(),
            )
            .expect("live stable channels bind and acquire event resources");
        channels.terminal();

        bound
            .events
            .send(SupervisorEvent::SupervisorStarted)
            .expect("bound incarnation retains its event sender");
        assert!(matches!(
            events.try_recv(),
            Ok(SupervisorEvent::SupervisorStarted)
        ));
    }

    #[test]
    fn terminalization_during_initial_handoff_rejects_binding_without_panicking() {
        let initial_snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            Vec::new(),
        );
        let channels = StableSupervisorChannels::new(
            initial_snapshot.clone(),
            8,
            8,
            empty_nested_channels(),
            Vec::new(),
        );
        let handle = channels.handle();
        let initial = channels
            .take_initial_incarnation(0)
            .expect("initial incarnation channels are available");

        channels.terminal();

        assert!(
            channels
                .bind(
                    0,
                    initial.shutdown_tx,
                    initial.command_tx,
                    initial.done_rx,
                    initial_snapshot,
                    Vec::new(),
                )
                .is_none(),
            "a terminal identity must not be rebound"
        );
        assert!(matches!(
            handle.control_endpoint(),
            Err(ControlError::Unavailable)
        ));
        assert_eq!(handle.snapshot().state, SupervisorStateView::Running);
    }
}

pub(crate) struct StableBindingGuard {
    channels: Arc<StableSupervisorChannels>,
    generation: u64,
}

pub(crate) struct BoundIncarnation {
    pub(crate) guard: StableBindingGuard,
    pub(crate) snapshots: watch::Sender<SupervisorSnapshot>,
    pub(crate) events: broadcast::Sender<SupervisorEvent>,
    pub(crate) lifecycle: Arc<LifecycleHub>,
}

impl Drop for StableBindingGuard {
    fn drop(&mut self) {
        let mut binding = self
            .channels
            .binding
            .lock()
            .expect("stable control slot poisoned");
        if binding
            .current
            .as_ref()
            .is_some_and(|binding| binding.generation == self.generation)
            && let Some(binding) = binding.current.take()
        {
            let _ = binding.shutdown_tx.send(true);
            // A revivable stable identity can remain observable between
            // incarnations. Invalidate the old incarnation's attachments now
            // so an ancestor rebinding cannot traverse stale descendants
            // before this supervisor binds and publishes its replacement
            // membership view.
            *self
                .channels
                .attached_children
                .lock()
                .expect("attached child view poisoned") = AttachedChildrenView {
                generation: None,
                terminal: false,
                children: Vec::new(),
            };
            self.channels.bump_binding_revision();
        }
    }
}

pub(crate) struct RootExtra {
    done_rx: DoneReceiver,
    join_state: Arc<Mutex<Option<(SupervisorJoinHandle, DoneSender)>>>,
}

impl RootExtra {
    pub(crate) fn new(
        done_rx: DoneReceiver,
        join_handle: SupervisorJoinHandle,
        done_tx: DoneSender,
    ) -> Self {
        Self {
            done_rx,
            join_state: Arc::new(Mutex::new(Some((join_handle, done_tx)))),
        }
    }
}

pub(crate) fn empty_nested_channels() -> NestedChannels {
    Arc::new(Mutex::new(HashMap::new()))
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
        supervisor: PendingSupervisorSpec,
        reply: oneshot::Sender<Result<u64, ControlError>>,
    },
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
///   for state, and [`watch_lifecycle`](Self::watch_lifecycle) for ordered,
///   reliable transitions.
/// - **Completion**: [`wait`](Self::wait) to await the supervisor's exit.
///
/// For a spawned root, dropping the last public handle clone requests graceful
/// shutdown, equivalent to calling [`shutdown`](Self::shutdown). Other root
/// clones keep the supervision tree alive, so fire-and-forget operation
/// requires retaining one. A scoped stable handle for a nested supervisor does
/// not own that supervisor's lifecycle; dropping it leaves the parent-owned
/// child running.
/// [`wait`](Self::wait) does not resolve until the supervisor has drained and
/// joined its child tasks.
#[derive(Clone)]
pub struct SupervisorHandle {
    channels: Arc<StableSupervisorChannels>,
    _lease: Option<Arc<HandleLease>>,
}

struct HandleLease {
    channels: Weak<StableSupervisorChannels>,
}

impl Drop for HandleLease {
    fn drop(&mut self) {
        let Some(channels) = self.channels.upgrade() else {
            return;
        };
        {
            // `handle()` mints a replacement lease as soon as this one's
            // strong count reaches zero, which happens before this `drop`
            // body runs. Without re-checking the slot under its own lock, a
            // handle taken concurrently with the last drop — a pre-spawn
            // handle released while `spawn` acquires its own — would be
            // followed by this shutdown signal against the live binding.
            let slot = channels
                .handle_lease
                .lock()
                .expect("handle lease slot poisoned");
            if !std::ptr::eq(slot.as_ptr(), self) {
                return;
            }
        }
        if matches!(channels.root_extra(), RootExtraSlot::NotRoot) {
            return;
        }
        if let Some(binding) = channels.current_binding() {
            let _ = binding.shutdown_tx.send(true);
        }
    }
}

impl std::fmt::Debug for SupervisorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorHandle").finish_non_exhaustive()
    }
}

impl SupervisorHandle {
    /// Requests a graceful shutdown of the supervisor.
    ///
    /// This is non-blocking: it signals the supervisor to begin its shutdown
    /// sequence and returns immediately. Use [`wait`](Self::wait) or
    /// [`shutdown_and_wait`](Self::shutdown_and_wait) to await completion.
    ///
    /// Calling `shutdown` multiple times is harmless.
    pub fn shutdown(&self) {
        if let Some(binding) = self.channels.current_binding() {
            let _ = binding.shutdown_tx.send(true);
        }
    }

    /// Requests a graceful shutdown and waits for the supervisor to fully stop.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.shutdown();
        self.wait().await
    }

    /// Returns a clone that observes and controls this supervisor without
    /// holding its lifecycle lease.
    ///
    /// Background tasks owned by the supervision machinery use this so that
    /// retaining one does not, by itself, keep a spawned root alive: dropping
    /// the last *public* handle clone must still request shutdown.
    pub(crate) fn observer(&self) -> Self {
        Self {
            channels: Arc::clone(&self.channels),
            _lease: None,
        }
    }

    /// Adds a new child to the supervisor at runtime.
    ///
    /// Waits if the control channel is full. On success, returns the membership
    /// epoch assigned to the child by the supervisor. The epoch is allocated
    /// atomically with insertion, so it identifies the membership created by
    /// this specific call even if the same child id is later removed and
    /// reused. Success means the membership was inserted and its start was
    /// scheduled. This operation is supported only by dynamic supervisors,
    /// which spawn it immediately. Use [`wait_started`](Self::wait_started)
    /// when readiness is required.
    pub async fn add_child(&self, child: ChildSpec) -> Result<u64, ControlError> {
        self.control_endpoint()?.add_child(child).await
    }

    /// Adds a nested supervisor at runtime with restart-stable observation and
    /// control channels.
    ///
    /// On success, returns the membership epoch assigned atomically with the
    /// insertion. The nested handle and attachment are registered at insertion,
    /// before the child is spawned. This operation is supported only by
    /// dynamic supervisors; use [`wait_started`](Self::wait_started) to await
    /// readiness.
    pub async fn add_supervisor(&self, supervisor: SupervisorSpec) -> Result<u64, ControlError> {
        let supervisor = PendingSupervisorSpec::new(supervisor);
        let endpoint = self.control_endpoint()?;
        endpoint.add_supervisor(supervisor).await
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
        let mut binding_revision = self.channels.binding_revision_rx();
        let (mut done_rx, join) = loop {
            match self.channels.root_extra() {
                RootExtraSlot::Ready(root) => {
                    if let Some(result) = root.done_rx.borrow().clone() {
                        return result;
                    }
                    let join = root
                        .join_state
                        .lock()
                        .expect("join_state mutex poisoned")
                        .take();
                    break (root.done_rx.clone(), join);
                }
                RootExtraSlot::Pending => {
                    binding_revision.changed().await.map_err(|_| {
                        SupervisorError::Internal(
                            "root-extra publication channel closed".to_owned(),
                        )
                    })?;
                }
                RootExtraSlot::NotRoot => match self.channels.wait_target() {
                    WaitTarget::Bound(done_rx) => break (done_rx, None),
                    // A reserved identity that has never bound has not
                    // started, so waiting for it to stop means waiting for it
                    // to run first. This mirrors `wait_started`, which also
                    // waits out the pre-bind window rather than failing.
                    WaitTarget::NeverBound => {
                        binding_revision.changed().await.map_err(|_| {
                            SupervisorError::Internal(
                                "supervisor binding channel closed before startup".to_owned(),
                            )
                        })?;
                    }
                    WaitTarget::BetweenIncarnations => {
                        return Err(SupervisorError::Internal(
                            "supervisor incarnation is unavailable".to_owned(),
                        ));
                    }
                    WaitTarget::Terminal => {
                        return Err(SupervisorError::Internal(
                            "supervisor identity is terminal and cannot run again".to_owned(),
                        ));
                    }
                },
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
    /// empty supervisor is ready immediately only after it has bound; a
    /// pre-bind empty supervisor waits for its first incarnation to bind.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorError::StartupAborted`] if a gated child exits or
    /// the supervisor stops before readiness is reported.
    pub async fn wait_started(&self) -> Result<(), SupervisorError> {
        let mut snapshots = self.snapshots_rx();
        let mut binding_revision = self.channels.binding_revision_rx();
        loop {
            let snapshot = match self.channels.startup_snapshot(&mut snapshots) {
                StartupSnapshot::Bound(snapshot) => snapshot,
                StartupSnapshot::Terminal => {
                    return Err(SupervisorError::StartupAborted(
                        "supervisor became terminal before startup".to_owned(),
                    ));
                }
                StartupSnapshot::Unbound => {
                    binding_revision.changed().await.map_err(|_| {
                        SupervisorError::StartupAborted(
                            "supervisor binding channel closed before startup".to_owned(),
                        )
                    })?;
                    continue;
                }
            };
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
            tokio::select! {
                changed = snapshots.changed() => changed.map_err(|_| {
                    SupervisorError::StartupAborted(
                        "supervisor stopped before all children reported readiness".to_owned(),
                    )
                })?,
                changed = binding_revision.changed() => changed.map_err(|_| {
                    SupervisorError::StartupAborted(
                        "supervisor binding channel closed before startup".to_owned(),
                    )
                })?,
            }
        }
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
    /// - [`watch_lifecycle`](Self::watch_lifecycle) for ordered transitions, or
    /// - [`subscribe_snapshots`](Self::subscribe_snapshots) with the
    ///   monotonic [`SupervisorSnapshot::total_restarts`] and per-child
    ///   [`ChildSnapshot::restart_count`](crate::ChildSnapshot::restart_count)
    ///   counters. Snapshots are delivered over a `watch` channel, which
    ///   conflates intermediate values but never lags, so counter deltas
    ///   account for every restart.
    pub fn subscribe(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.channels.events_rx()
    }

    /// Returns an ordered, reliable stream of lifecycle transitions among
    /// this supervisor's direct children.
    ///
    /// The baseline is creation time: earlier transitions are not replayed.
    /// To obtain a gap-free state-plus-stream view, create the watch first,
    /// then read [`snapshot`](Self::snapshot), then discard watched events
    /// whose sequence is at most [`SupervisorSnapshot::lifecycle_seq`].
    /// Pre-spawn snapshots already project configured children as `Starting`,
    /// so apply a later `Added` for that membership as an idempotent upsert
    /// keyed by `(child_id, membership_epoch)`. Membership epochs remain
    /// unique across incarnations of this stable supervisor identity.
    ///
    /// Each watch owns a bounded buffer. Sustained overflow is represented by
    /// [`LifecycleEventKind::Lagged`](crate::LifecycleEventKind::Lagged), never
    /// silent loss. This scope does not aggregate nested supervisors; obtain a
    /// nested handle with [`supervisor`](Self::supervisor) and watch it
    /// separately.
    pub fn watch_lifecycle(&self) -> LifecycleWatch {
        self.lifecycle_hub().watch()
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
        self.channels
            .current_binding()
            .map(|binding| binding.control)
            .ok_or(ControlError::Unavailable)
    }

    fn snapshots_rx(&self) -> watch::Receiver<SupervisorSnapshot> {
        self.channels.snapshots_rx()
    }

    fn lifecycle_hub(&self) -> Arc<LifecycleHub> {
        self.channels.lifecycle()
    }

    fn nested_channels(&self) -> NestedChannels {
        self.channels.nested_channels()
    }

    fn attached_children_state(&self) -> AttachedChildrenState {
        self.channels.attached_children()
    }
}
