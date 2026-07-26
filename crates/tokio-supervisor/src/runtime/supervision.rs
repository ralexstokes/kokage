use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

use slab::Slab;
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::{Id, JoinError, JoinSet},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::{
    child::{ChildDefinition, ChildKind, ChildReadiness, OpaqueAttachment},
    context::ChildReady,
    error::{ControlError, SupervisorError},
    event::{ExitStatusView, NestedEventNotification, SupervisorEvent},
    handle::{
        AttachedChildState, AttachedChildrenState, NestedChannels, PendingSupervisorSpec,
        StableSupervisorChannels, SupervisorCommand, SupervisorHandle,
    },
    lifecycle::{LifecycleEventDraft, LifecycleEventKind, LifecycleHub},
    observability::{SupervisorObservability, format_child_path},
    restart::{RestartIntensity, RestartPolicy},
    scope::{ControlOperation, ScopeKind},
    shutdown::{AutoShutdown, ShutdownMode, ShutdownPolicy},
    snapshot::{
        ChildMembershipView, ChildSnapshot, ChildStateView, NestedSnapshotNotification,
        NestedSnapshotState, SupervisorSnapshot, SupervisorStateView,
    },
    strategy::Strategy,
    supervisor::{ParentLink, SupervisorConfig},
};

use super::{
    child_runtime::{ChildRuntime, RuntimeChildState},
    exit::ExitStatus,
};

/// Slab key for a child entry. Stable across restarts but invalidated when the
/// child is removed from the slab.
pub(crate) type ChildKey = usize;

/// Message returned by a child task through the `JoinSet`. Task identity is
/// correlated through `task_map`, including for successful joins.
pub(crate) struct ChildEnvelope {
    pub(crate) result: crate::child::ChildResult,
}

/// Metadata stored alongside a Tokio task ID so every join result can be
/// mapped back to the originating child.
#[derive(Clone, Copy)]
pub(crate) struct TaskMeta {
    pub(crate) key: ChildKey,
    pub(crate) instance: u64,
    pub(crate) generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SupervisorState {
    Running,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipState {
    Active,
    Removing,
    Removed,
}

enum ExitReason {
    Shutdown,
    Failure(SupervisorError),
}

impl From<SupervisorError> for ExitReason {
    fn from(error: SupervisorError) -> Self {
        Self::Failure(error)
    }
}

type RuntimeResult<T> = Result<T, ExitReason>;

struct CommandFailure {
    error: ControlError,
    exit: Option<ExitReason>,
}

impl From<ControlError> for CommandFailure {
    fn from(error: ControlError) -> Self {
        Self { error, exit: None }
    }
}

impl From<ExitReason> for CommandFailure {
    fn from(exit: ExitReason) -> Self {
        let error = match &exit {
            ExitReason::Shutdown => ControlError::SupervisorStopping,
            ExitReason::Failure(error) => map_supervisor_error_to_control(error.clone()),
        };
        Self {
            error,
            exit: Some(exit),
        }
    }
}

type CommandResult<T> = Result<T, CommandFailure>;
type JoinedChild = Result<(Id, ChildEnvelope), JoinError>;

#[derive(Clone, Copy)]
struct WakeOptions {
    commands: bool,
    nested_snapshots: bool,
    nested_events: bool,
    readiness: bool,
    joins: bool,
    deadline: Option<Instant>,
}

impl WakeOptions {
    fn main_loop(deadline: Option<Instant>) -> Self {
        Self {
            commands: true,
            nested_snapshots: true,
            nested_events: true,
            readiness: true,
            joins: true,
            deadline,
        }
    }
}

enum Wake {
    Shutdown,
    Command(Option<SupervisorCommand>),
    NestedSnapshot(Option<NestedSnapshotNotification>),
    NestedEvent(Option<NestedEventNotification>),
    Ready(Option<ChildReady>),
    Joined(Option<JoinedChild>),
    Deadline,
}

/// Per-child bookkeeping entry stored in the supervisor's slab.
///
/// `instance` is a monotonically increasing identifier that distinguishes
/// different memberships in one restart-stable supervisor identity (e.g.
/// after a child is removed and a new one is inserted at the recycled key, or
/// after the supervisor itself is reincarnated). Combined with `generation`
/// (which counts restarts of the *same* child spec), this pair identifies every
/// task the supervisor has spawned unless the counter reaches its saturating
/// `u64::MAX` limit. The instance is exposed to observers as
/// [`ChildSnapshot::membership_epoch`].
pub(crate) struct ChildEntry {
    pub(crate) id: String,
    pub(crate) formatted_path: String,
    /// Monotonic membership instance. See struct-level docs.
    pub(crate) instance: u64,
    pub(crate) attachment: Option<OpaqueAttachment>,
    pub(crate) runtime: ChildRuntime,
    last_exit: Option<ExitStatusView>,
    pub(crate) nested_snapshot: Option<SupervisorSnapshot>,
    pub(crate) nested_snapshot_state: Option<NestedSnapshotState>,
    pub(crate) nested_channels: Option<Arc<StableSupervisorChannels>>,
    pub(crate) membership: MembershipState,
    pending_removal: Option<PendingRemoval>,
}

struct PendingRemoval {
    reply: oneshot::Sender<Result<(), ControlError>>,
    mode: ShutdownMode,
    grace_deadline: Instant,
    initiated_at: StdInstant,
    grace_expired: bool,
}

#[derive(Clone, Copy)]
struct StartItem {
    key: ChildKey,
    instance: u64,
    emit_restart_event: bool,
}

struct StartGate {
    key: ChildKey,
    instance: u64,
    generation: u64,
}

#[derive(Default)]
struct StartSequence {
    queue: VecDeque<StartItem>,
    gate: Option<StartGate>,
}

struct StartupGate {
    ready: crate::context::ChildContext,
    pending: Vec<(ChildKey, u64)>,
}

impl ChildEntry {
    fn new(
        id: String,
        formatted_path: String,
        definition: Arc<ChildDefinition>,
        nested_channels: Option<Arc<StableSupervisorChannels>>,
        default_restart_intensity: RestartIntensity,
        instance: u64,
    ) -> Self {
        Self {
            id,
            formatted_path,
            instance,
            attachment: definition.attachment.clone(),
            runtime: ChildRuntime::new(definition, default_restart_intensity),
            last_exit: None,
            nested_snapshot: None,
            nested_snapshot_state: None,
            nested_channels,
            membership: MembershipState::Active,
            pending_removal: None,
        }
    }
}

/// Reconciles the stable identities retained from a previous incarnation with
/// the nested supervisors in the static configuration.
///
/// Static identities are reused. Missing identities are recreated, while
/// dynamic identities that collide with static children or are absent from
/// the new incarnation are made terminal.
pub(crate) fn reconcile_stable_identities(
    children: &[Arc<ChildDefinition>],
    nested_channels: &NestedChannels,
) -> HashMap<String, Arc<StableSupervisorChannels>> {
    let mut identities = HashMap::new();
    let mut displaced = Vec::new();
    let mut channel_map = nested_channels.lock().expect("nested channel map poisoned");

    for child in children {
        let ChildKind::Supervisor(supervisor) = &child.kind else {
            continue;
        };

        // A dynamically added child that happens to share the id is a
        // different identity and must not be conflated with the recreated
        // static child.
        let reusable = channel_map
            .get(&child.id)
            .filter(|channels| channels.statically_configured())
            .cloned();
        let stable = reusable.unwrap_or_else(|| {
            // The identity is missing (a previous incarnation removed this
            // static child) or occupied by a dynamic child. Mint a fresh
            // static identity and displace any dynamic occupant.
            let stable = supervisor.stable_channels(true);
            if let Some(occupant) = channel_map.insert(child.id.clone(), Arc::clone(&stable)) {
                displaced.push(occupant);
            }
            stable
        });
        identities.insert(child.id.clone(), stable);
    }

    // Anything else was added dynamically in a previous incarnation. The
    // replacement incarnation will never spawn it, including when its id now
    // belongs to a static task, so close the identity instead of leaving its
    // observers hanging.
    let orphaned_ids: Vec<String> = channel_map
        .keys()
        .filter(|id| !identities.contains_key(*id))
        .cloned()
        .collect();
    let orphaned = orphaned_ids
        .into_iter()
        .filter_map(|id| channel_map.remove(&id))
        .collect::<Vec<_>>();
    drop(channel_map);

    for channels in orphaned.into_iter().chain(displaced) {
        channels.terminal();
    }

    identities
}

/// Read-only configuration and identity, fixed at construction time.
pub(crate) struct RuntimeMeta {
    pub(crate) strategy: Strategy,
    pub(crate) kind: ScopeKind,
    pub(crate) auto_shutdown: AutoShutdown,
    pub(crate) default_restart_intensity: RestartIntensity,
    pub(crate) default_restart: RestartPolicy,
    pub(crate) default_shutdown: ShutdownPolicy,
    pub(crate) path_prefix: Vec<String>,
    pub(crate) observability: SupervisorObservability,
    pub(crate) parent_link: Option<ParentLink>,
    /// Whether this supervisor incarnation could ever be replaced by another
    /// one: it is restartable by its parent, or some ancestor is revivable
    /// *along a statically configured chain* (reincarnation respawns static
    /// children only; dynamically added children are orphaned). A root
    /// supervisor is never revivable. Terminality judgments about statically
    /// configured children are final only when this is `false` — a revivable
    /// supervisor's replacement incarnation recreates them.
    pub(crate) revivable: bool,
}

/// Core state machine that drives the supervisor's select loop.
///
/// # Key invariants
///
/// - `live_tasks` tracks children that have a live Tokio task (i.e. an
///   `abort_handle` was stored). It is decremented in `consume_joined_child`
///   and `finalize_removed_child`. When it reaches zero during shutdown the
///   drain loop exits.
/// - `child_order` preserves insertion order for deterministic snapshot output
///   and `OneForAll` restart sequencing.
/// - `task_map` maps Tokio `Id` → `TaskMeta` so all join results are
///   attributed to the correct child generation in one place.
pub(crate) struct SupervisorRuntime {
    pub(crate) meta: RuntimeMeta,
    pub(crate) state: SupervisorState,
    /// Supervisor-level stop observation. This is separate from
    /// `group_token`: ordered shutdown must cancel children one at a time,
    /// while every child must still observe the supervisor entering its
    /// stopping state immediately.
    pub(crate) stopping_token: CancellationToken,
    /// Parent token whose children are the per-child tokens. Cancelling this
    /// token cancels all children at once (used in shutdown and `OneForAll`
    /// restarts).
    pub(crate) group_token: CancellationToken,
    pub(crate) join_set: JoinSet<ChildEnvelope>,
    pub(crate) children: Slab<ChildEntry>,
    pub(crate) children_by_id: HashMap<String, ChildKey>,
    pub(crate) child_order: Vec<ChildKey>,
    pub(crate) live_tasks: usize,
    pub(crate) events: broadcast::Sender<SupervisorEvent>,
    pub(crate) lifecycle: Arc<LifecycleHub>,
    pub(crate) snapshots: watch::Sender<SupervisorSnapshot>,
    pub(crate) attached_children: AttachedChildrenState,
    pub(crate) shutdown_rx: watch::Receiver<bool>,
    pub(crate) command_rx: mpsc::Receiver<SupervisorCommand>,
    pub(crate) nested_channels: NestedChannels,
    pub(crate) own_handle: SupervisorHandle,
    pub(crate) nested_event_tx: mpsc::Sender<NestedEventNotification>,
    pub(crate) nested_event_rx: mpsc::Receiver<NestedEventNotification>,
    pub(crate) nested_snapshot_tx: mpsc::UnboundedSender<NestedSnapshotNotification>,
    pub(crate) nested_snapshot_rx: mpsc::UnboundedReceiver<NestedSnapshotNotification>,
    pub(crate) ready_tx: mpsc::UnboundedSender<ChildReady>,
    pub(crate) ready_rx: mpsc::UnboundedReceiver<ChildReady>,
    pub(crate) commands_open: bool,
    pub(crate) task_map: HashMap<Id, TaskMeta>,
    start_sequence: Option<StartSequence>,
    startup_gate: Option<StartupGate>,
    /// Cumulative restarts scheduled across all direct children (including
    /// since-removed ones), seeded from the previous incarnation for nested
    /// supervisors; exposed as [`SupervisorSnapshot::total_restarts`].
    pub(crate) total_restarts: u64,
}

impl SupervisorRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: SupervisorConfig,
        shutdown_rx: watch::Receiver<bool>,
        events: broadcast::Sender<SupervisorEvent>,
        lifecycle: Arc<LifecycleHub>,
        snapshots: watch::Sender<SupervisorSnapshot>,
        attached_children: AttachedChildrenState,
        command_rx: mpsc::Receiver<SupervisorCommand>,
        nested_channels: NestedChannels,
        path_prefix: Vec<String>,
        parent_link: Option<ParentLink>,
        revivable: bool,
        own_handle: SupervisorHandle,
    ) -> Self {
        let default_restart_intensity = config.restart_intensity;
        let kind = config.kind;
        let observability = SupervisorObservability::new(path_prefix.clone(), config.strategy);
        let mut children = Slab::with_capacity(config.children.len());
        let mut children_by_id = HashMap::with_capacity(config.children.len());
        let mut child_order = Vec::with_capacity(config.children.len());
        let declared_membership_epochs: HashMap<_, _> = snapshots
            .borrow()
            .children
            .iter()
            .map(|child| (child.id.clone(), child.membership_epoch))
            .collect();
        let mut stable_identities = reconcile_stable_identities(&config.children, &nested_channels);

        for spec in config.children {
            let id = spec.id.clone();
            let formatted_path = format_child_path(&path_prefix, &id);
            let child_nested_channels = match &spec.kind {
                ChildKind::Supervisor(_) => Some(
                    stable_identities
                        .remove(&id)
                        .expect("static supervisor identity was reconciled"),
                ),
                ChildKind::Task(_) => None,
            };
            let membership_epoch = *declared_membership_epochs
                .get(&id)
                .expect("initial snapshot contains every static child");
            lifecycle.observe_membership_epoch(membership_epoch);
            let key = children.insert(ChildEntry::new(
                id.clone(),
                formatted_path,
                spec,
                child_nested_channels,
                default_restart_intensity,
                membership_epoch,
            ));
            children_by_id.insert(id.clone(), key);
            child_order.push(key);
        }
        // Nested incarnations reuse the stable snapshot channel. Binding the
        // new incarnation reset its child state while preserving the previous
        // incarnation's cumulative restart counter in that snapshot; a fresh
        // root channel holds the initial snapshot (zero). Resume from that
        // preserved value to keep `total_restarts` monotonic.
        let total_restarts = snapshots.borrow().total_restarts;
        // At most one notification per nested child can be queued because
        // `NestedSnapshotState` coalesces updates behind an atomic flag.
        let (nested_snapshot_tx, nested_snapshot_rx) = mpsc::unbounded_channel();
        let (nested_event_tx, nested_event_rx) = mpsc::channel(config.event_channel_capacity);
        let (ready_tx, ready_rx) = mpsc::unbounded_channel();

        Self {
            meta: RuntimeMeta {
                strategy: config.strategy,
                kind,
                auto_shutdown: config.auto_shutdown,
                default_restart_intensity,
                default_restart: config.default_restart,
                default_shutdown: config.default_shutdown,
                path_prefix,
                observability,
                parent_link,
                revivable,
            },
            state: SupervisorState::Running,
            stopping_token: CancellationToken::new(),
            group_token: CancellationToken::new(),
            join_set: JoinSet::new(),
            children,
            children_by_id,
            child_order,
            live_tasks: 0,
            events,
            lifecycle,
            snapshots,
            attached_children,
            shutdown_rx,
            command_rx,
            nested_channels,
            own_handle,
            nested_event_tx,
            nested_event_rx,
            nested_snapshot_tx,
            nested_snapshot_rx,
            ready_tx,
            ready_rx,
            commands_open: true,
            task_map: HashMap::new(),
            start_sequence: None,
            startup_gate: None,
            total_restarts,
        }
    }

    pub(crate) async fn run(
        &mut self,
        startup_ready: Option<crate::context::ChildContext>,
    ) -> Result<(), SupervisorError> {
        match self.run_until_exit(startup_ready).await {
            Ok(()) => {
                self.resolve_pending_removals(None);
                Ok(())
            }
            Err(ExitReason::Shutdown) => {
                let result = self.shutdown_all().await;
                self.resolve_pending_removals(None);
                result
            }
            Err(ExitReason::Failure(error @ SupervisorError::StartupAborted(_))) => {
                let _ = self.shutdown_all().await;
                self.resolve_pending_removals(Some(&error));
                Err(error)
            }
            Err(ExitReason::Failure(error)) => {
                // A parent-restartable incarnation preserves stable nested
                // lifecycle continuity by letting its nested runtimes finish
                // cooperatively before the replacement binds. A terminal
                // root (or otherwise non-revivable incarnation) has no future
                // supervisor above those runtimes, so keep the hard cascade
                // armed instead of detaching them past `wait()`.
                if self.meta.revivable {
                    self.detach_nested_children_for_revivable_failure();
                }
                self.resolve_pending_removals(Some(&error));
                Err(error)
            }
        }
    }

    fn detach_nested_children_for_revivable_failure(&self) {
        for (_, child) in self.children.iter() {
            if child.runtime.state.is_active()
                && matches!(child.runtime.definition.kind, ChildKind::Supervisor(_))
            {
                child
                    .runtime
                    .nested_abort_cascades
                    .store(false, std::sync::atomic::Ordering::Release);
            }
        }
    }

    async fn run_until_exit(
        &mut self,
        startup_ready: Option<crate::context::ChildContext>,
    ) -> RuntimeResult<()> {
        self.publish_snapshot();
        self.send_event(SupervisorEvent::SupervisorStarted);
        let initial_children = self.child_order.clone();
        for &key in &initial_children {
            self.send_lifecycle(key, LifecycleEventKind::Added);
        }
        let initial_instances: Vec<_> = initial_children
            .iter()
            .filter_map(|&key| self.children.get(key).map(|entry| (key, entry.instance)))
            .collect();
        if let Some(startup_ready) = startup_ready {
            self.startup_gate = Some(StartupGate {
                ready: startup_ready,
                pending: initial_instances,
            });
        }
        self.start_children(initial_children)?;
        self.complete_empty_startup_gate();

        loop {
            match self
                .next_wake(WakeOptions::main_loop(self.earliest_deadline()))
                .await
            {
                Wake::Shutdown => return Err(ExitReason::Shutdown),
                Wake::Command(command) => match command {
                    Some(command) => self.handle_command(command)?,
                    None => self.commands_open = false,
                },
                Wake::NestedSnapshot(update) => {
                    if let Some(update) = update {
                        self.handle_nested_snapshot(update);
                    }
                }
                Wake::NestedEvent(event) => {
                    if let Some(event) = event {
                        self.handle_nested_event(event);
                    }
                }
                Wake::Ready(ready) => {
                    if let Some(ready) = ready {
                        self.handle_child_ready(ready)?;
                    }
                }
                Wake::Joined(maybe) => {
                    if let Some(joined) = maybe {
                        self.handle_joined_child(joined)?;
                    }
                }
                Wake::Deadline => {
                    // Preserve the existing zero-backoff contract: let tasks
                    // woken by `ChildRestartScheduled` enqueue control work,
                    // then give the whole queued batch priority over an
                    // already-due restart.
                    tokio::task::yield_now().await;
                    self.drain_deadline_command_batch()?;
                    self.handle_deadlines().await?;
                }
            }
        }
    }

    fn drain_deadline_command_batch(&mut self) -> RuntimeResult<()> {
        loop {
            // The batch preserves command ordering relative to the due
            // restart, but shutdown retains the select loop's higher priority
            // between every pair of commands.
            if self.shutdown_rx.has_changed().is_err() || *self.shutdown_rx.borrow() {
                return Err(ExitReason::Shutdown);
            }
            match self.command_rx.try_recv() {
                Ok(command) => self.handle_command(command)?,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // `try_recv` reports disconnection only after every
                    // buffered command has been consumed.
                    self.commands_open = false;
                    return Ok(());
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(()),
            }
        }
    }

    /// Waits for the next enabled runtime input. Shutdown is always enabled
    /// and always wins when multiple inputs are ready.
    async fn next_wake(&mut self, options: WakeOptions) -> Wake {
        loop {
            if self.shutdown_rx.has_changed().is_err() || *self.shutdown_rx.borrow() {
                return Wake::Shutdown;
            }
            if options
                .deadline
                .is_some_and(|deadline| deadline <= Instant::now())
            {
                return Wake::Deadline;
            }
            let wait_for_join = options.joins && !self.join_set.is_empty();
            let deadline = options.deadline.unwrap_or_else(Instant::now);
            let wake = tokio::select! {
                biased;
                changed = self.shutdown_rx.changed() => {
                    self.shutdown_requested(changed).then_some(Wake::Shutdown)
                }
                command = self.command_rx.recv(), if options.commands && self.commands_open => {
                    Some(Wake::Command(command))
                }
                ready = self.ready_rx.recv(), if options.readiness => {
                    Some(Wake::Ready(ready))
                }
                update = self.nested_snapshot_rx.recv(), if options.nested_snapshots => {
                    Some(Wake::NestedSnapshot(update))
                }
                event = self.nested_event_rx.recv(), if options.nested_events => {
                    Some(Wake::NestedEvent(event))
                }
                joined = self.join_set.join_next_with_id(), if wait_for_join => {
                    Some(Wake::Joined(joined))
                }
                _ = tokio::time::sleep_until(deadline), if options.deadline.is_some() => {
                    Some(Wake::Deadline)
                }
            };
            if let Some(wake) = wake {
                return wake;
            }
        }
    }

    fn start_children(&mut self, keys: Vec<ChildKey>) -> RuntimeResult<()> {
        self.schedule_start_sequence(keys, false)
    }

    fn respawn_sequence(
        &mut self,
        keys: Vec<ChildKey>,
        emit_restart_events: bool,
    ) -> RuntimeResult<()> {
        self.schedule_start_sequence(keys, emit_restart_events)
    }

    fn schedule_start_sequence(
        &mut self,
        keys: Vec<ChildKey>,
        emit_restart_events: bool,
    ) -> RuntimeResult<()> {
        for key in keys {
            if emit_restart_events {
                match self.group_respawn_disposition(key) {
                    GroupRespawnDisposition::Respawn => {}
                    GroupRespawnDisposition::Skip => continue,
                    GroupRespawnDisposition::Finalize { startup_aborted } => {
                        self.finalize_skipped_group_respawn(key, startup_aborted)?;
                        continue;
                    }
                }
            }

            let Some(entry) = self.children.get(key) else {
                continue;
            };
            if entry.membership != MembershipState::Active
                || !matches!(
                    entry.runtime.state,
                    RuntimeChildState::Stopped | RuntimeChildState::StartQueued
                )
            {
                continue;
            }

            let item = StartItem {
                key,
                instance: entry.instance,
                emit_restart_event: emit_restart_events,
            };
            if self.meta.kind == ScopeKind::Dynamic {
                self.spawn_start_item(item)?;
            } else {
                let entry = &mut self.children[key];
                entry.runtime.state = RuntimeChildState::StartQueued;
                entry.runtime.has_reported_ready = false;
                entry.runtime.next_restart_deadline = None;
                self.start_sequence
                    .get_or_insert_with(StartSequence::default)
                    .queue
                    .push_back(item);
            }
        }

        if self.meta.kind == ScopeKind::Ordered {
            self.publish_snapshot();
            self.advance_start_sequence()?;
        }
        Ok(())
    }

    fn spawn_start_item(&mut self, item: StartItem) -> RuntimeResult<()> {
        let Some(entry) = self.children.get(item.key) else {
            return Ok(());
        };
        if entry.instance != item.instance
            || entry.membership != MembershipState::Active
            || !matches!(
                entry.runtime.state,
                RuntimeChildState::Stopped | RuntimeChildState::StartQueued
            )
        {
            return Ok(());
        }
        let readiness_gated = entry.runtime.definition.readiness == ChildReadiness::Explicit;
        let (old_generation, new_generation) = self.spawn_child(item.key)?;
        if self.meta.kind == ScopeKind::Ordered && readiness_gated {
            self.start_sequence
                .get_or_insert_with(StartSequence::default)
                .gate = Some(StartGate {
                key: item.key,
                instance: item.instance,
                generation: new_generation,
            });
        } else if !readiness_gated {
            self.startup_member_ready(item.key, item.instance);
        }
        if item.emit_restart_event
            && let Some(old_generation) = old_generation
        {
            // Group-restart transitions are emitted when the replacement is
            // spawned, matching one-for-one restarts and preserving the full
            // generation chain even if readiness is never reported.
            self.send_restart_event(item.key, old_generation, new_generation);
        }
        Ok(())
    }

    fn advance_start_sequence(&mut self) -> RuntimeResult<()> {
        loop {
            let gate_action = self.start_sequence.as_ref().and_then(|sequence| {
                sequence.gate.as_ref().map(|gate| {
                    let state = self.children.get(gate.key).and_then(|entry| {
                        (entry.instance == gate.instance
                            && entry.membership == MembershipState::Active)
                            .then_some((entry.runtime.state, entry.runtime.next_restart_deadline))
                    });
                    (gate.key, gate.instance, state)
                })
            });

            if let Some((key, instance, state)) = gate_action {
                match state {
                    Some((RuntimeChildState::Starting | RuntimeChildState::Stopping, _))
                    | Some((RuntimeChildState::Stopped, Some(_))) => return Ok(()),
                    Some((RuntimeChildState::Running, _)) => {
                        self.start_sequence
                            .as_mut()
                            .and_then(|sequence| sequence.gate.take())
                            .expect("start gate was present");
                        self.startup_member_ready(key, instance);
                    }
                    Some((RuntimeChildState::Stopped, None)) => {
                        self.start_sequence
                            .as_mut()
                            .expect("start sequence was present")
                            .gate = None;
                        self.startup_member_aborted(key, instance)?;
                    }
                    Some((RuntimeChildState::StartQueued, _)) | None => {
                        self.start_sequence
                            .as_mut()
                            .expect("start sequence was present")
                            .gate = None;
                    }
                }
                continue;
            }

            let item = self
                .start_sequence
                .as_mut()
                .and_then(|sequence| sequence.queue.pop_front());
            let Some(item) = item else {
                self.start_sequence = None;
                return Ok(());
            };
            self.spawn_start_item(item)?;
            if self
                .start_sequence
                .as_ref()
                .is_some_and(|sequence| sequence.gate.is_some())
            {
                return Ok(());
            }
        }
    }

    fn startup_member_ready(&mut self, key: ChildKey, instance: u64) {
        let Some(gate) = self.startup_gate.as_mut() else {
            return;
        };
        gate.pending.retain(|&(pending_key, pending_instance)| {
            pending_key != key || pending_instance != instance
        });
        self.complete_empty_startup_gate();
    }

    fn startup_member_aborted(&mut self, key: ChildKey, instance: u64) -> RuntimeResult<()> {
        let pending = self
            .startup_gate
            .as_ref()
            .is_some_and(|gate| gate.pending.contains(&(key, instance)));
        if !pending {
            return Ok(());
        }
        let id = self
            .children
            .get(key)
            .map_or_else(|| "unknown".to_owned(), |entry| entry.id.clone());
        Err(SupervisorError::StartupAborted(format!(
            "child `{id}` exited before reporting readiness"
        ))
        .into())
    }

    fn startup_member_removed(&mut self, key: ChildKey, instance: u64) {
        self.startup_member_ready(key, instance);
    }

    fn complete_empty_startup_gate(&mut self) {
        if self
            .startup_gate
            .as_ref()
            .is_some_and(|gate| gate.pending.is_empty())
        {
            let gate = self.startup_gate.take().expect("startup gate was present");
            gate.ready.mark_ready();
        }
    }

    fn handle_child_ready(&mut self, ready: ChildReady) -> RuntimeResult<()> {
        let Some(entry) = self.children.get_mut(ready.key) else {
            return Ok(());
        };
        if entry.instance != ready.instance
            || entry.runtime.generation != ready.generation
            || entry.membership != MembershipState::Active
            || entry.runtime.state != RuntimeChildState::Starting
        {
            return Ok(());
        }
        entry.runtime.state = RuntimeChildState::Running;
        entry.runtime.has_reported_ready = true;
        let id = entry.id.clone();
        self.send_lifecycle(
            ready.key,
            LifecycleEventKind::Started {
                generation: ready.generation,
            },
        );
        self.send_event(SupervisorEvent::ChildStarted {
            id,
            generation: ready.generation,
        });
        self.startup_member_ready(ready.key, ready.instance);

        let matches_gate = self.start_sequence.as_ref().is_some_and(|sequence| {
            sequence.gate.as_ref().is_some_and(|gate| {
                gate.key == ready.key
                    && gate.instance == ready.instance
                    && gate.generation == ready.generation
            })
        });
        if matches_gate {
            self.start_sequence
                .as_mut()
                .and_then(|sequence| sequence.gate.take())
                .expect("matching start gate was present");
            self.advance_start_sequence()?;
        }
        Ok(())
    }

    fn group_respawn_disposition(&self, key: ChildKey) -> GroupRespawnDisposition {
        let Some(entry) = self.children.get(key) else {
            return GroupRespawnDisposition::Skip;
        };
        if entry.membership != MembershipState::Active {
            return GroupRespawnDisposition::Skip;
        }
        if entry.runtime.has_started
            && matches!(entry.runtime.definition.restart, RestartPolicy::Never)
        {
            return GroupRespawnDisposition::Finalize {
                startup_aborted: !entry.runtime.has_reported_ready,
            };
        }
        GroupRespawnDisposition::Respawn
    }

    fn finalize_skipped_group_respawn(
        &mut self,
        key: ChildKey,
        startup_aborted: bool,
    ) -> RuntimeResult<()> {
        let instance = self.children[key].instance;
        if startup_aborted {
            self.children[key].runtime.startup_aborted = true;
            self.publish_snapshot();
            self.startup_member_aborted(key, instance)?;
        }
        // Skipped by the group respawn and never restarted afterwards; if
        // this supervisor is the root, that judgment is final.
        if self.children[key].runtime.definition.remove_on_exit {
            self.finalize_removed_child(key, false);
        } else {
            self.mark_child_terminal(key);
        }
        Ok(())
    }

    fn handle_command(&mut self, command: SupervisorCommand) -> RuntimeResult<()> {
        match command {
            SupervisorCommand::AddChild { child, reply } => {
                complete_command(reply, self.add_child(child))
            }
            SupervisorCommand::RemoveChild { id, reply } => self.remove_child(id, reply),
            SupervisorCommand::AddSupervisor {
                id,
                supervisor,
                reply,
            } => complete_command(reply, self.add_supervisor(id, supervisor)),
        }
    }

    fn add_child(&mut self, mut child: crate::child::ChildSpec) -> CommandResult<u64> {
        if self.state == SupervisorState::Stopping {
            return Err(ControlError::SupervisorStopping.into());
        }
        if self.meta.kind == ScopeKind::Ordered {
            return Err(ControlError::UnsupportedByScopeKind {
                operation: ControlOperation::AddChild,
                kind: self.meta.kind,
            }
            .into());
        }
        if child.is_significant() {
            return Err(ControlError::InvalidConfig(
                "dynamic scopes do not support significant children",
            )
            .into());
        }

        Arc::make_mut(&mut child.inner)
            .apply_defaults(self.meta.default_restart, self.meta.default_shutdown);

        if child.id().is_empty() {
            return Err(ControlError::InvalidConfig("child id must not be empty").into());
        }

        if let Some(restart_intensity) = child.restart_intensity_override() {
            restart_intensity
                .validate()
                .map_err(|err| map_build_error_to_control(child.id(), err))?;
        }
        let id = child.id().to_owned();
        if let Some(&key) = self.children_by_id.get(&id) {
            let error = if self.children[key].membership == MembershipState::Removing {
                ControlError::ChildRemovalInProgress(id)
            } else {
                ControlError::DuplicateChildId(id)
            };
            return Err(error.into());
        }

        let formatted_path = format_child_path(&self.meta.path_prefix, &id);
        let definition = child.inner;
        let membership_epoch = self.lifecycle.next_membership_epoch();
        let key = self.children.insert(ChildEntry::new(
            id.clone(),
            formatted_path,
            definition,
            None,
            self.meta.default_restart_intensity,
            membership_epoch,
        ));
        self.children_by_id.insert(id.clone(), key);
        self.child_order.push(key);
        self.send_lifecycle(key, LifecycleEventKind::Added);

        self.start_children(vec![key])?;

        Ok(membership_epoch)
    }

    fn add_supervisor(
        &mut self,
        id: String,
        mut pending: PendingSupervisorSpec,
    ) -> CommandResult<u64> {
        if self.state == SupervisorState::Stopping {
            return Err(ControlError::SupervisorStopping.into());
        }
        if self.meta.kind == ScopeKind::Ordered {
            return Err(ControlError::UnsupportedByScopeKind {
                operation: ControlOperation::AddSupervisor,
                kind: self.meta.kind,
            }
            .into());
        }
        // Every early return from here on drops `pending`, which terminalizes
        // the identity its caller reserved.
        let spec = pending.spec_mut();
        if spec.significant {
            return Err(ControlError::InvalidConfig(
                "dynamic scopes do not support significant children",
            )
            .into());
        }
        spec.apply_defaults(self.meta.default_restart, self.meta.default_shutdown);
        if id.is_empty() {
            return Err(ControlError::InvalidConfig("child id must not be empty").into());
        }
        if let Some(intensity) = spec.restart_intensity {
            intensity
                .validate()
                .map_err(|error| map_build_error_to_control(&id, error))?;
        }
        if let Some(&key) = self.children_by_id.get(&id) {
            let error = if self.children[key].membership == MembershipState::Removing {
                ControlError::ChildRemovalInProgress(id)
            } else {
                ControlError::DuplicateChildId(id)
            };
            return Err(error.into());
        }

        let stable = pending.spec_mut().supervisor.stable_channels(false);
        let supervisor = pending.accept();
        let definition = Arc::new(ChildDefinition::supervisor(id.clone(), supervisor));
        let formatted_path = format_child_path(&self.meta.path_prefix, &id);
        let membership_epoch = self.lifecycle.next_membership_epoch();
        let key = self.children.insert(ChildEntry::new(
            id.clone(),
            formatted_path,
            definition,
            Some(Arc::clone(&stable)),
            self.meta.default_restart_intensity,
            membership_epoch,
        ));
        self.children_by_id.insert(id.clone(), key);
        self.child_order.push(key);
        self.nested_channels
            .lock()
            .expect("nested channel map poisoned")
            .insert(id.clone(), stable);
        self.send_lifecycle(key, LifecycleEventKind::Added);

        self.start_children(vec![key])?;

        Ok(membership_epoch)
    }

    pub(crate) fn finish(&mut self) {
        self.state = SupervisorState::Stopped;
        self.send_event(SupervisorEvent::SupervisorStopped);
    }

    /// Called once the runtime loop has exited (graceful stop or fatal
    /// error). A non-revivable supervisor can never run again, so every
    /// nested child is terminal: close their stable channels. A revivable
    /// supervisor's stop may be a restart cycle, so its statically
    /// configured children stay open (its parent cascades terminality when
    /// it will never be respawned) — but its dynamically added children are
    /// closed either way: a replacement incarnation orphans them rather
    /// than respawning them.
    pub(crate) fn finalize_stable_channels(&self) {
        for (key, _) in self.children.iter() {
            self.mark_child_terminal(key);
        }
    }

    fn handle_nested_snapshot(&mut self, notification: NestedSnapshotNotification) {
        let Some(entry) = self.children.get_mut(notification.parent_key) else {
            return;
        };
        if entry.instance != notification.parent_instance
            || entry.runtime.generation != notification.generation
        {
            return;
        }

        let Some(state) = entry.nested_snapshot_state.clone() else {
            return;
        };
        state.mark_dequeued();
        entry.nested_snapshot = state.latest();
        self.publish_snapshot();
    }

    fn handle_nested_event(&mut self, notification: NestedEventNotification) {
        let Some(entry) = self.children.get_mut(notification.parent_key) else {
            return;
        };
        if entry.instance != notification.parent_instance
            || entry.runtime.generation != notification.generation
        {
            return;
        }

        if let Some(state) = entry.nested_snapshot_state.as_ref() {
            entry.nested_snapshot = state.latest();
        }
        self.send_event(SupervisorEvent::Nested {
            id: notification.id,
            generation: notification.generation,
            event: Box::new(notification.event),
        });
    }

    fn remove_child(
        &mut self,
        id: String,
        reply: oneshot::Sender<Result<(), ControlError>>,
    ) -> RuntimeResult<()> {
        if self.state == SupervisorState::Stopping {
            let _ = reply.send(Err(ControlError::SupervisorStopping));
            return Ok(());
        }
        if self.meta.kind == ScopeKind::Ordered {
            let _ = reply.send(Err(ControlError::UnsupportedByScopeKind {
                operation: ControlOperation::RemoveChild,
                kind: self.meta.kind,
            }));
            return Ok(());
        }

        let Some(&key) = self.children_by_id.get(&id) else {
            let _ = reply.send(Err(ControlError::UnknownChildId(id)));
            return Ok(());
        };

        if self.children[key].membership == MembershipState::Removing {
            let _ = reply.send(Err(ControlError::ChildRemovalInProgress(id)));
            return Ok(());
        }

        let (instance, mode, active) = {
            let entry = &mut self.children[key];
            entry.membership = MembershipState::Removing;
            let active = entry.runtime.state.is_active();
            if active {
                entry.runtime.state = RuntimeChildState::Stopping;
            }
            let mode = entry.runtime.definition.shutdown_policy.mode;
            let grace_deadline = Instant::now() + entry.runtime.definition.shutdown_policy.grace;
            entry.pending_removal = Some(PendingRemoval {
                reply,
                mode,
                grace_deadline,
                initiated_at: StdInstant::now(),
                grace_expired: false,
            });
            (entry.instance, mode, active)
        };

        self.publish_snapshot();
        if active {
            match mode {
                ShutdownMode::Abort => self.abort_child(key),
                ShutdownMode::CooperativeStrict | ShutdownMode::CooperativeThenAbort => {
                    self.cancel_child(key)
                }
            }
        }
        self.detach_start_member(key, instance)?;

        if !active {
            self.finalize_removed_child(key, false);
        }
        Ok(())
    }

    fn detach_start_member(&mut self, key: ChildKey, instance: u64) -> RuntimeResult<()> {
        let cleared_gate = self.remove_start_sequence_member(key, instance);
        self.startup_member_removed(key, instance);
        if cleared_gate {
            self.advance_start_sequence()?;
        }
        Ok(())
    }

    fn terminal_start_member(&mut self, key: ChildKey, instance: u64) -> RuntimeResult<()> {
        let cleared_gate = self.remove_start_sequence_member(key, instance);
        self.startup_member_aborted(key, instance)?;
        if cleared_gate {
            self.advance_start_sequence()?;
        }
        Ok(())
    }

    fn remove_start_sequence_member(&mut self, key: ChildKey, instance: u64) -> bool {
        let mut cleared_gate = false;
        if let Some(sequence) = self.start_sequence.as_mut() {
            sequence
                .queue
                .retain(|item| item.key != key || item.instance != instance);
            if sequence
                .gate
                .as_ref()
                .is_some_and(|gate| gate.key == key && gate.instance == instance)
            {
                sequence.gate = None;
                cleared_gate = true;
            }
        }
        cleared_gate
    }

    pub(crate) fn cancel_child(&mut self, key: ChildKey) {
        self.children[key].runtime.completion.mark_cancelled();
        if let Some(token) = self.children[key].runtime.active_token.as_ref() {
            token.cancel();
        }
    }

    pub(crate) fn abort_child(&mut self, key: ChildKey) {
        let child = &self.children[key].runtime;
        child.completion.mark_cancelled();
        child
            .nested_abort_cascades
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(abort_handle) = child.abort_handle.as_ref() {
            abort_handle.abort();
        }
    }

    fn shutdown_requested(
        &self,
        changed: Result<(), tokio::sync::watch::error::RecvError>,
    ) -> bool {
        match changed {
            Ok(()) => *self.shutdown_rx.borrow(),
            Err(_) => true,
        }
    }

    pub(crate) fn child_path(&self, key: ChildKey) -> Vec<String> {
        let mut path = self.meta.path_prefix.clone();
        path.push(self.children[key].id.clone());
        path
    }

    pub(crate) fn finalize_removed_child(&mut self, key: ChildKey, check_elapsed_grace: bool) {
        if !self.children.contains(key) {
            return;
        }

        let lifecycle = self.lifecycle_draft(key, LifecycleEventKind::Removed);
        let had_live_task = self.children[key].runtime.abort_handle.is_some();
        let mut entry = self.children.remove(key);
        let pending_removal = entry.pending_removal.take();
        entry.membership = MembershipState::Removed;
        entry.last_exit = None;
        entry.nested_snapshot = None;
        if let Some(state) = entry.nested_snapshot_state.as_ref() {
            state.clear();
        }
        if had_live_task {
            self.live_tasks = self.live_tasks.saturating_sub(1);
        }
        self.children_by_id.remove(&entry.id);
        self.child_order.retain(|&existing| existing != key);
        if matches!(&entry.runtime.definition.kind, ChildKind::Supervisor(_)) {
            self.nested_channels
                .lock()
                .expect("nested channel map poisoned")
                .remove(&entry.id);
            if let Some(channels) = entry.nested_channels.as_ref() {
                channels.terminal();
            }
        }
        let id = entry.id.clone();
        // Dropping a task definition may emit its terminal lifecycle signal.
        // Do that before publishing removal so watches observe terminality
        // before the child disappears from membership.
        drop(entry);
        if let Some(lifecycle) = lifecycle {
            self.send_lifecycle_draft(lifecycle);
        }
        self.send_event(SupervisorEvent::ChildRemoved { id: id.clone() });

        if let Some(pending) = pending_removal {
            let result = self.pending_removal_result(&id, &pending, check_elapsed_grace, None);
            // Removal completion is deliberately last: terminality, the
            // membership drop, and ChildRemoved are all observable first.
            let _ = pending.reply.send(result);
        }
    }

    /// Resolves every accepted removal whose ordinary child-exit path did not
    /// run before the supervisor loop exited. Keeping this epilogue on every
    /// exit prevents reply senders from being dropped as `Unavailable`.
    fn resolve_pending_removals(&mut self, failure: Option<&SupervisorError>) {
        let pending: Vec<_> = self
            .children
            .iter_mut()
            .filter_map(|(_, entry)| {
                entry
                    .pending_removal
                    .take()
                    .map(|pending| (entry.id.clone(), pending))
            })
            .collect();
        for (id, pending) in pending {
            let result = self.pending_removal_result(&id, &pending, true, failure);
            let _ = pending.reply.send(result);
        }
    }

    fn pending_removal_result(
        &self,
        id: &str,
        pending: &PendingRemoval,
        check_elapsed_grace: bool,
        failure: Option<&SupervisorError>,
    ) -> Result<(), ControlError> {
        let grace_expired = pending.grace_expired
            || (check_elapsed_grace
                && !matches!(pending.mode, ShutdownMode::Abort)
                && Instant::now() >= pending.grace_deadline);
        if grace_expired && !pending.grace_expired {
            self.meta
                .observability
                .record_shutdown_timeout("remove_child", Some(id));
        }
        self.meta.observability.record_shutdown_duration(
            "remove_child",
            pending.initiated_at.elapsed(),
            Some(id),
        );
        if let Some(error) = failure {
            Err(map_supervisor_error_to_control(error.clone()))
        } else if grace_expired && matches!(pending.mode, ShutdownMode::CooperativeStrict) {
            Err(ControlError::ShutdownTimedOut(id.to_owned()))
        } else {
            Ok(())
        }
    }

    fn handle_joined_child(
        &mut self,
        joined: Result<(Id, ChildEnvelope), JoinError>,
    ) -> RuntimeResult<()> {
        let Some(classified) = self.consume_joined_child(joined)? else {
            return Ok(());
        };
        self.dispatch_exit(classified)
    }

    fn dispatch_exit(&mut self, classified: ClassifiedExit) -> RuntimeResult<()> {
        self.record_exit(classified.key, classified.generation, &classified.status);
        self.apply_exit_policy(classified)
    }

    fn apply_exit_policy(&mut self, classified: ClassifiedExit) -> RuntimeResult<()> {
        self.apply_exit_policy_inner(classified, true)
    }

    fn apply_drained_completion_policy(&mut self, classified: ClassifiedExit) -> RuntimeResult<()> {
        self.apply_exit_policy_inner(classified, false)
    }

    fn apply_exit_policy_inner(
        &mut self,
        classified: ClassifiedExit,
        allow_restart: bool,
    ) -> RuntimeResult<()> {
        if self.state != SupervisorState::Running {
            return Ok(());
        }

        if self.children[classified.key].membership == MembershipState::Removing {
            self.finalize_removed_child(classified.key, false);
            return Ok(());
        }

        if self.auto_shutdown_triggered(classified.key, &classified.status) {
            let id = self.children[classified.key].id.clone();
            self.send_event(SupervisorEvent::AutoShutdownTriggered {
                id,
                mode: self.meta.auto_shutdown,
            });
            return Err(ExitReason::Shutdown);
        }

        let restart_policy = self.children[classified.key].runtime.definition.restart;

        if allow_restart && restart_policy.should_restart(classified.status.is_failure()) {
            let previous_generation = self.children[classified.key].runtime.generation;
            let delay = self.schedule_restart(classified.key)?;
            self.send_event(SupervisorEvent::ChildRestartScheduled {
                id: self.children[classified.key].id.clone(),
                generation: previous_generation,
                delay,
            });
        } else if allow_restart {
            let instance = self.children[classified.key].instance;
            let startup_aborted = !self.children[classified.key].runtime.has_reported_ready;
            if startup_aborted {
                self.children[classified.key].runtime.startup_aborted = true;
                self.publish_snapshot();
            }
            if self.children[classified.key]
                .runtime
                .definition
                .remove_on_exit
            {
                let startup_result = if startup_aborted {
                    self.terminal_start_member(classified.key, instance)
                } else {
                    Ok(())
                };
                self.finalize_removed_child(classified.key, false);
                startup_result?;
                return Ok(());
            }
            // The exit will not be restarted. Under group strategies a
            // stopped non-`Never` child can still be respawned by a later
            // sibling-triggered group restart, so finality needs more than
            // the exit itself: a `OneForOne` stop and a `Never` policy are
            // always final, and under `RestForOne` so is a stop of the first
            // child — a group restart respawns only the suffix from the
            // failing position, and nothing precedes the first. Children at
            // other group positions conservatively stay open (an earlier
            // sibling's failure could revive them) until the supervisor
            // stops.
            let stop_is_final = match self.meta.strategy {
                Strategy::OneForOne => true,
                Strategy::OneForAll => matches!(restart_policy, RestartPolicy::Never),
                Strategy::RestForOne => {
                    matches!(restart_policy, RestartPolicy::Never)
                        || self.child_order.first() == Some(&classified.key)
                }
            };
            if stop_is_final {
                self.mark_child_terminal(classified.key);
            }
            if startup_aborted {
                self.terminal_start_member(classified.key, instance)?;
            }
        }

        Ok(())
    }

    /// Marks a permanently stopped nested-supervisor child's stable channels
    /// as terminal. No-op for task children.
    ///
    /// A revivable supervisor's judgment is provisional only for its
    /// statically configured children: an ancestor reincarnation recreates
    /// those from the static configuration with the same stable channels, so
    /// they must stay open. A dynamically added child is orphaned by
    /// reincarnation instead, so its stop is final either way.
    fn mark_child_terminal(&self, key: ChildKey) {
        let Some(entry) = self.children.get(key) else {
            return;
        };
        let Some(channels) = entry.nested_channels.as_ref() else {
            return;
        };
        if self.meta.revivable && channels.statically_configured() {
            return;
        }
        channels.terminal();
    }

    fn auto_shutdown_triggered(&self, exited_key: ChildKey, status: &ExitStatus) -> bool {
        if !matches!(status, ExitStatus::Completed)
            || !self.children[exited_key].runtime.definition.significant
        {
            return false;
        }

        match self.meta.auto_shutdown {
            AutoShutdown::Never => false,
            AutoShutdown::AnySignificant => true,
            AutoShutdown::AllSignificant => self.children.iter().all(|(_, child)| {
                if child.membership != MembershipState::Active
                    || !child.runtime.definition.significant
                {
                    return true;
                }
                !child.runtime.state.is_active()
                    && child.runtime.completion.is_clean()
                    && matches!(child.last_exit, Some(ExitStatusView::Completed))
            }),
        }
    }

    fn classify_join(
        &mut self,
        joined: Result<(Id, ChildEnvelope), JoinError>,
    ) -> Result<ClassifiedExit, SupervisorError> {
        match joined {
            Ok((task_id, envelope)) => {
                let Some(meta) = self.task_map.remove(&task_id) else {
                    return Err(SupervisorError::Internal(format!(
                        "missing task metadata for successful join: {task_id:?}"
                    )));
                };
                Ok(ClassifiedExit {
                    key: meta.key,
                    instance: meta.instance,
                    generation: meta.generation,
                    status: ExitStatus::from_child_result(envelope.result),
                })
            }
            Err(err) => {
                let task_id = err.id();
                let Some(meta) = self.task_map.remove(&task_id) else {
                    return Err(SupervisorError::Internal(format!(
                        "missing task metadata for failed join: {err}"
                    )));
                };
                let status = classify_join_error(err);
                Ok(ClassifiedExit {
                    key: meta.key,
                    instance: meta.instance,
                    generation: meta.generation,
                    status,
                })
            }
        }
    }

    pub(crate) fn record_exit(&mut self, key: ChildKey, generation: u64, status: &ExitStatus) {
        let id = {
            let entry = &mut self.children[key];
            entry.runtime.restart_tracker.record_exit(Instant::now());
            entry.runtime.state = RuntimeChildState::Stopped;
            entry.runtime.active_token = None;
            entry.runtime.abort_handle = None;
            entry.runtime.next_restart_deadline = None;
            entry.last_exit = Some(status.view());
            entry.nested_snapshot = None;
            entry.nested_snapshot_state = None;
            entry.id.clone()
        };
        self.send_lifecycle(
            key,
            LifecycleEventKind::Exited {
                generation,
                reason: status.view(),
            },
        );
        self.send_event(SupervisorEvent::ChildExited {
            id,
            generation,
            status: status.view(),
        });
    }

    fn earliest_deadline(&self) -> Option<Instant> {
        self.children
            .iter()
            .flat_map(|(_, entry)| {
                let removal = entry.pending_removal.as_ref().and_then(|pending| {
                    (!pending.grace_expired && !matches!(pending.mode, ShutdownMode::Abort))
                        .then_some(pending.grace_deadline)
                });
                [removal, entry.runtime.next_restart_deadline]
            })
            .flatten()
            .min()
    }

    async fn handle_deadlines(&mut self) -> RuntimeResult<()> {
        // A shutdown request always wins over restart and removal deadlines. In
        // particular, the deadline wake path yields once so a concurrently
        // queued shutdown can be observed before a zero-delay restart spawns.
        if self.shutdown_rx.has_changed().is_err() || *self.shutdown_rx.borrow() {
            return Err(ExitReason::Shutdown);
        }

        let now = Instant::now();
        let expired_removals: Vec<_> = self
            .child_order
            .iter()
            .copied()
            .filter(|&key| {
                self.children.get(key).is_some_and(|entry| {
                    entry.pending_removal.as_ref().is_some_and(|pending| {
                        !pending.grace_expired
                            && !matches!(pending.mode, ShutdownMode::Abort)
                            && pending.grace_deadline <= now
                    })
                })
            })
            .collect();
        for key in expired_removals {
            let id = {
                let entry = &mut self.children[key];
                let pending = entry
                    .pending_removal
                    .as_mut()
                    .expect("expired removal retained its pending state");
                pending.grace_expired = true;
                entry.id.clone()
            };
            self.meta
                .observability
                .record_shutdown_timeout("remove_child", Some(&id));
            self.abort_child(key);
        }

        let due_restarts: Vec<_> = self
            .child_order
            .iter()
            .copied()
            .filter(|&key| {
                self.children.get(key).is_some_and(|entry| {
                    entry.membership == MembershipState::Active
                        && entry.runtime.state == RuntimeChildState::Stopped
                        && entry
                            .runtime
                            .next_restart_deadline
                            .is_some_and(|deadline| deadline <= now)
                })
            })
            .collect();

        match self.meta.strategy {
            Strategy::OneForOne => {
                for key in due_restarts {
                    self.restart_one(key)?;
                }
            }
            Strategy::OneForAll => {
                if let Some(key) = due_restarts.first().copied() {
                    self.children[key].runtime.next_restart_deadline = None;
                    self.restart_group(key, true).await?;
                }
            }
            Strategy::RestForOne => {
                if let Some(key) = due_restarts.first().copied() {
                    self.children[key].runtime.next_restart_deadline = None;
                    let deferred = self.restart_group(key, false).await?;
                    for classified in deferred {
                        if self.current_child_matches(
                            classified.key,
                            classified.instance,
                            classified.generation,
                        ) {
                            self.apply_exit_policy(classified)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn restart_one(&mut self, key: ChildKey) -> RuntimeResult<()> {
        let Some(entry) = self.children.get(key) else {
            return Ok(());
        };
        if entry.membership != MembershipState::Active
            || entry.runtime.state != RuntimeChildState::Stopped
            || entry.runtime.next_restart_deadline.is_none()
        {
            return Ok(());
        }
        let instance = entry.instance;
        self.children[key].runtime.next_restart_deadline = None;
        let (old_generation, new_generation) = self.spawn_child(key)?;
        if let Some(gate) = self
            .start_sequence
            .as_mut()
            .and_then(|sequence| sequence.gate.as_mut())
            .filter(|gate| gate.key == key && gate.instance == instance)
        {
            gate.generation = new_generation;
        }
        if let Some(old_generation) = old_generation {
            self.send_restart_event(key, old_generation, new_generation);
        }
        if self.children[key].runtime.state == RuntimeChildState::Running {
            self.advance_start_sequence()?;
        }
        Ok(())
    }

    async fn restart_group(
        &mut self,
        failing_key: ChildKey,
        fresh_group_token: bool,
    ) -> RuntimeResult<Vec<ClassifiedExit>> {
        let Some(failing_entry) = self.children.get(failing_key) else {
            return Ok(Vec::new());
        };
        if failing_entry.membership != MembershipState::Active {
            return Ok(Vec::new());
        }
        let keys = if fresh_group_token {
            self.child_order.clone()
        } else {
            let Some(failing_position) =
                self.child_order.iter().position(|&key| key == failing_key)
            else {
                return Ok(Vec::new());
            };
            self.child_order[failing_position..].to_vec()
        };
        let failing_id = self.children[failing_key].id.clone();
        if fresh_group_token {
            debug!("restarting child group after exit from {failing_id}");
        } else {
            debug!("restarting child suffix after exit from {failing_id}");
        }

        // OneForAll drains the old generation completely before creating a
        // fresh group token, so old and new tasks never overlap. RestForOne
        // cancels and drains only the selected suffix.
        let completed = if fresh_group_token {
            self.drain_for_group_restart().await?
        } else {
            self.drain_for_rest_for_one_restart(&keys).await?
        };
        let (completed_in_scope, deferred): (Vec<_>, Vec<_>) = completed
            .into_iter()
            .partition(|classified| keys.contains(&classified.key));
        for classified in completed_in_scope {
            if !self.current_child_matches(
                classified.key,
                classified.instance,
                classified.generation,
            ) {
                continue;
            }
            self.apply_drained_completion_policy(classified)?;
        }
        if fresh_group_token {
            self.group_token = CancellationToken::new();
        }
        if let Some(sequence) = self.start_sequence.as_mut() {
            sequence.queue.retain(|item| !keys.contains(&item.key));
            if sequence
                .gate
                .as_ref()
                .is_some_and(|gate| keys.contains(&gate.key))
            {
                sequence.gate = None;
            }
        }
        self.respawn_sequence(keys, true)?;
        Ok(deferred)
    }

    fn schedule_restart(&mut self, key: ChildKey) -> Result<Duration, SupervisorError> {
        self.total_restarts = self.total_restarts.saturating_add(1);
        let delay = {
            let now = Instant::now();
            let child = &mut self.children[key].runtime;
            child.restart_tracker.record_restart(now);
            if child.restart_tracker.exceeded() {
                None
            } else {
                let delay = child.restart_tracker.backoff();
                child.next_restart_deadline = Some(now + delay);
                Some(delay)
            }
        };

        let Some(delay) = delay else {
            self.send_event(SupervisorEvent::RestartIntensityExceeded);
            return Err(SupervisorError::RestartIntensityExceeded);
        };

        let child_id = &*self.children[key].id;
        trace!(?child_id, ?delay, "scheduled child restart");
        Ok(delay)
    }

    fn running_child_count(&self) -> usize {
        self.children
            .iter()
            .filter(|(_, child)| counts_as_running(child.membership, child.runtime.state))
            .count()
    }

    fn send_restart_event(&self, key: ChildKey, old_generation: u64, new_generation: u64) {
        let Some(entry) = self.children.get(key) else {
            return;
        };
        if entry.runtime.generation != new_generation {
            return;
        }
        self.send_event(SupervisorEvent::ChildRestarted {
            id: entry.id.clone(),
            old_generation,
            new_generation,
        });
    }

    pub(crate) fn send_lifecycle(&self, key: ChildKey, kind: LifecycleEventKind) {
        if let Some(draft) = self.lifecycle_draft(key, kind) {
            self.send_lifecycle_draft(draft);
        }
    }

    fn lifecycle_draft(
        &self,
        key: ChildKey,
        kind: LifecycleEventKind,
    ) -> Option<LifecycleEventDraft> {
        let entry = self.children.get(key)?;
        Some(LifecycleEventDraft {
            child_id: entry.id.clone(),
            membership_epoch: entry.instance,
            total_restarts: self.total_restarts,
            child_restart_count: entry.runtime.restart_tracker.total_restarts(),
            kind,
        })
    }

    fn send_lifecycle_draft(&self, draft: LifecycleEventDraft) {
        let lifecycle = Arc::clone(&self.lifecycle);
        lifecycle.emit(draft, || self.publish_snapshot());
    }

    pub(crate) fn send_event(&self, event: SupervisorEvent) {
        if event_updates_snapshot(&event) {
            self.publish_snapshot();
        }
        let child_path = event_child_id(&event)
            .and_then(|id| self.children_by_id.get(id))
            .map(|&key| self.children[key].formatted_path.as_str());
        self.meta
            .observability
            .emit_event(&event, self.running_child_count(), child_path);
        let _ = self.events.send(event.clone());
        if let Some(parent_link) = self.meta.parent_link.as_ref() {
            parent_link.forward_event(event);
        }
    }

    pub(crate) fn publish_snapshot(&self) {
        let mut attached_children = self
            .attached_children
            .lock()
            .expect("attached child view poisoned");
        let generation = self.meta.parent_link.as_ref().map(|link| link.generation);
        if !attached_children.terminal && attached_children.generation == generation {
            attached_children.children = self.attached_children_view();
        }
        drop(attached_children);
        let snapshot = self.snapshot_view();
        self.snapshots.send_if_modified(|current| {
            if *current == snapshot {
                return false;
            }
            current.clone_from(&snapshot);
            true
        });
        if let Some(parent_link) = self.meta.parent_link.as_ref() {
            parent_link.publish_snapshot(snapshot);
        }
    }

    fn attached_children_view(&self) -> Vec<AttachedChildState> {
        self.child_order
            .iter()
            .filter_map(|&key| self.children.get(key))
            .map(|entry| AttachedChildState {
                identity: crate::AttachedChildIdentity {
                    id: entry.id.clone(),
                    membership_epoch: entry.instance,
                    generation: entry.runtime.generation,
                },
                attachment: entry.attachment.clone(),
                supervisor: entry
                    .nested_channels
                    .as_ref()
                    .map(|channels| channels.internal_handle()),
            })
            .collect()
    }

    fn snapshot_view(&self) -> SupervisorSnapshot {
        let now = Instant::now();
        let mut children = Vec::with_capacity(self.children_by_id.len());
        for &key in &self.child_order {
            let Some(entry) = self.children.get(key) else {
                continue;
            };

            children.push(ChildSnapshot {
                id: entry.id.clone(),
                membership_epoch: entry.instance,
                generation: entry.runtime.generation,
                started: entry.runtime.has_reported_ready,
                startup_aborted: entry.runtime.startup_aborted,
                state: match entry.runtime.state {
                    RuntimeChildState::StartQueued | RuntimeChildState::Starting => {
                        ChildStateView::Starting
                    }
                    RuntimeChildState::Running => ChildStateView::Running,
                    RuntimeChildState::Stopping => ChildStateView::Stopping,
                    RuntimeChildState::Stopped => ChildStateView::Stopped,
                },
                membership: match entry.membership {
                    MembershipState::Active => ChildMembershipView::Active,
                    MembershipState::Removing => ChildMembershipView::Removing,
                    MembershipState::Removed => unreachable!("removed children filtered"),
                },
                last_exit: entry.last_exit.clone(),
                restart_count: entry.runtime.restart_tracker.total_restarts(),
                next_restart_in: entry
                    .runtime
                    .next_restart_deadline
                    .map(|deadline| deadline.saturating_duration_since(now)),
                supervisor: entry.nested_snapshot.as_ref().cloned().map(Box::new),
            });
        }

        SupervisorSnapshot {
            state: match self.state {
                SupervisorState::Running => SupervisorStateView::Running,
                SupervisorState::Stopping => SupervisorStateView::Stopping,
                SupervisorState::Stopped => SupervisorStateView::Stopped,
            },
            kind: self.meta.kind,
            strategy: self.meta.strategy,
            total_restarts: self.total_restarts,
            lifecycle_seq: self.lifecycle.seq(),
            children,
        }
    }

    pub(crate) fn consume_joined_child(
        &mut self,
        joined: Result<(Id, ChildEnvelope), JoinError>,
    ) -> Result<Option<ClassifiedExit>, SupervisorError> {
        let classified = self.classify_join(joined)?;
        if !self.current_child_matches(classified.key, classified.instance, classified.generation) {
            return Ok(None);
        }

        self.live_tasks = self.live_tasks.saturating_sub(1);
        Ok(Some(classified))
    }

    fn current_child_matches(&self, key: ChildKey, instance: u64, generation: u64) -> bool {
        self.children.get(key).is_some_and(|entry| {
            entry.instance == instance && entry.runtime.generation == generation
        })
    }
}

fn complete_command<T>(
    reply: oneshot::Sender<Result<T, ControlError>>,
    result: CommandResult<T>,
) -> RuntimeResult<()> {
    match result {
        Ok(value) => {
            let _ = reply.send(Ok(value));
            Ok(())
        }
        Err(CommandFailure { error, exit }) => {
            let _ = reply.send(Err(error));
            match exit {
                Some(exit) => Err(exit),
                None => Ok(()),
            }
        }
    }
}

fn classify_join_error(err: JoinError) -> ExitStatus {
    // Tokio reports aborts and cancellation through `is_cancelled`; any other
    // join error is treated as a panic from the child task.
    if err.is_cancelled() {
        ExitStatus::Aborted
    } else {
        ExitStatus::Panicked
    }
}

fn map_build_error_to_control(id: &str, err: crate::error::SupervisorBuildError) -> ControlError {
    match err {
        crate::error::SupervisorBuildError::DuplicateChildId(_) => {
            ControlError::DuplicateChildId(id.to_owned())
        }
        crate::error::SupervisorBuildError::InvalidConfig(message) => {
            ControlError::InvalidConfig(message)
        }
    }
}

fn map_supervisor_error_to_control(err: SupervisorError) -> ControlError {
    match err {
        SupervisorError::ShutdownTimedOut(ids) => ControlError::ShutdownTimedOut(ids),
        SupervisorError::Internal(message) => ControlError::Internal(message),
        SupervisorError::RestartIntensityExceeded | SupervisorError::StartupAborted(_) => {
            ControlError::SupervisorStopping
        }
    }
}

pub(crate) struct ClassifiedExit {
    pub(crate) key: ChildKey,
    instance: u64,
    pub(crate) generation: u64,
    pub(crate) status: ExitStatus,
}

enum GroupRespawnDisposition {
    Respawn,
    Skip,
    Finalize { startup_aborted: bool },
}

/// Why the supervisor is draining its join set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainReason {
    Shutdown,
    GroupRestart,
    RestForOneRestart,
}

fn counts_as_running(membership: MembershipState, state: RuntimeChildState) -> bool {
    membership != MembershipState::Removed
        && matches!(
            state,
            RuntimeChildState::Starting | RuntimeChildState::Running
        )
}

fn event_updates_snapshot(event: &SupervisorEvent) -> bool {
    !matches!(
        event,
        SupervisorEvent::SupervisorStarted | SupervisorEvent::ChildRestarted { .. }
    )
}

fn event_child_id(event: &SupervisorEvent) -> Option<&str> {
    match event {
        SupervisorEvent::ChildStarted { id, .. }
        | SupervisorEvent::ChildExited { id, .. }
        | SupervisorEvent::AutoShutdownTriggered { id, .. }
        | SupervisorEvent::ChildRestartScheduled { id, .. }
        | SupervisorEvent::ChildRestarted { id, .. }
        | SupervisorEvent::ChildRemoved { id }
        | SupervisorEvent::Nested { id, .. } => Some(id),
        SupervisorEvent::SupervisorStarted
        | SupervisorEvent::SupervisorStopping
        | SupervisorEvent::SupervisorStopped
        | SupervisorEvent::RestartIntensityExceeded => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChildSpec, Supervisor, SupervisorBuilder,
        handle::{attached_children_state, empty_nested_channels},
        supervisor::initial_snapshot,
    };

    fn empty_supervisor() -> Supervisor {
        SupervisorBuilder::new()
            .build()
            .expect("empty supervisor config is valid")
    }

    #[tokio::test]
    async fn readiness_wake_prioritizes_ready_over_nested_traffic() {
        let supervisor = empty_supervisor();
        let own_handle = supervisor.handle();
        let config = supervisor.config.clone();
        let event_capacity = config.event_channel_capacity;
        let control_capacity = config.control_channel_capacity;
        let initial_snapshot = initial_snapshot(&config);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (events_tx, _) = broadcast::channel(event_capacity);
        let (snapshots_tx, _) = watch::channel(initial_snapshot);
        let (_command_tx, command_rx) = mpsc::channel(control_capacity);
        let mut runtime = SupervisorRuntime::new(
            config,
            shutdown_rx,
            events_tx,
            LifecycleHub::new(),
            snapshots_tx,
            attached_children_state(None, Vec::new()),
            command_rx,
            empty_nested_channels(),
            Vec::new(),
            None,
            false,
            own_handle,
        );

        runtime
            .nested_event_tx
            .try_send(NestedEventNotification {
                parent_key: 0,
                parent_instance: 0,
                id: "noisy".to_owned(),
                generation: 0,
                event: SupervisorEvent::SupervisorStarted,
            })
            .expect("nested event channel should have capacity");
        runtime
            .ready_tx
            .send(ChildReady {
                key: 1,
                instance: 0,
                generation: 0,
            })
            .expect("readiness receiver should remain open");

        assert!(matches!(
            runtime.next_wake(WakeOptions::main_loop(None)).await,
            Wake::Ready(Some(ChildReady { key: 1, .. }))
        ));
    }

    #[test]
    fn deadline_command_batch_preserves_shutdown_priority() {
        let supervisor = empty_supervisor();
        let own_handle = supervisor.handle();
        let config = supervisor.config.clone();
        let event_capacity = config.event_channel_capacity;
        let control_capacity = config.control_channel_capacity;
        let initial_snapshot = initial_snapshot(&config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (events_tx, _) = broadcast::channel(event_capacity);
        let (snapshots_tx, _) = watch::channel(initial_snapshot);
        let (command_tx, command_rx) = mpsc::channel(control_capacity);
        let mut runtime = SupervisorRuntime::new(
            config,
            shutdown_rx,
            events_tx,
            LifecycleHub::new(),
            snapshots_tx,
            attached_children_state(None, Vec::new()),
            command_rx,
            empty_nested_channels(),
            Vec::new(),
            None,
            false,
            own_handle,
        );
        let (reply, _reply_rx) = oneshot::channel();
        command_tx
            .try_send(SupervisorCommand::AddChild {
                child: ChildSpec::new("late", |_| async { Ok(()) }),
                reply,
            })
            .expect("command channel should have capacity");
        shutdown_tx.send(true).expect("runtime retains receiver");

        assert!(matches!(
            runtime.drain_deadline_command_batch(),
            Err(ExitReason::Shutdown)
        ));
        assert!(
            !runtime.children_by_id.contains_key("late"),
            "queued commands must not be accepted after shutdown wins"
        );
    }

    #[test]
    fn pending_removal_epilogue_preserves_strict_grace_timeout() {
        let supervisor = SupervisorBuilder::new()
            .child(ChildSpec::new("removable", |_| async { Ok(()) }))
            .build()
            .expect("valid supervisor config");
        let own_handle = supervisor.handle();
        let config = supervisor.config.clone();
        let event_capacity = config.event_channel_capacity;
        let control_capacity = config.control_channel_capacity;
        let initial_snapshot = initial_snapshot(&config);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (events_tx, _) = broadcast::channel(event_capacity);
        let (snapshots_tx, _) = watch::channel(initial_snapshot);
        let (_command_tx, command_rx) = mpsc::channel(control_capacity);
        let mut runtime = SupervisorRuntime::new(
            config,
            shutdown_rx,
            events_tx,
            LifecycleHub::new(),
            snapshots_tx,
            attached_children_state(None, Vec::new()),
            command_rx,
            empty_nested_channels(),
            Vec::new(),
            None,
            false,
            own_handle,
        );
        let key = runtime.child_order[0];
        let (reply, mut reply_rx) = oneshot::channel();
        runtime.children[key].pending_removal = Some(PendingRemoval {
            reply,
            mode: ShutdownMode::CooperativeStrict,
            grace_deadline: Instant::now(),
            initiated_at: StdInstant::now(),
            grace_expired: true,
        });

        runtime.resolve_pending_removals(None);

        assert_eq!(
            reply_rx.try_recv(),
            Ok(Err(ControlError::ShutdownTimedOut("removable".to_owned())))
        );
    }

    #[tokio::test]
    async fn gated_group_restarts_emit_each_generation_before_readiness() {
        let supervisor = SupervisorBuilder::new()
            .child(
                ChildSpec::new("gated", |ctx| async move {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                })
                .wait_for_ready(),
            )
            .build()
            .expect("valid supervisor config");
        let own_handle = supervisor.handle();
        let config = supervisor.config.clone();
        let event_capacity = config.event_channel_capacity;
        let control_capacity = config.control_channel_capacity;
        let initial_snapshot = initial_snapshot(&config);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (events_tx, mut events_rx) = broadcast::channel(event_capacity);
        let (snapshots_tx, _) = watch::channel(initial_snapshot);
        let (_command_tx, command_rx) = mpsc::channel(control_capacity);
        let mut runtime = SupervisorRuntime::new(
            config,
            shutdown_rx,
            events_tx,
            LifecycleHub::new(),
            snapshots_tx,
            attached_children_state(None, Vec::new()),
            command_rx,
            empty_nested_channels(),
            Vec::new(),
            None,
            false,
            own_handle,
        );
        let key = runtime.child_order[0];
        let instance = runtime.children[key].instance;
        runtime.children[key].runtime.has_started = true;
        runtime.start_sequence = Some(StartSequence::default());

        assert!(
            runtime
                .spawn_start_item(StartItem {
                    key,
                    instance,
                    emit_restart_event: true,
                })
                .is_ok()
        );
        assert!(matches!(
            events_rx.try_recv(),
            Ok(SupervisorEvent::ChildRestarted {
                old_generation: 0,
                new_generation: 1,
                ..
            })
        ));

        {
            let child = &mut runtime.children[key].runtime;
            child.state = RuntimeChildState::Stopped;
            child.next_restart_deadline = Some(Instant::now());
        }

        assert!(runtime.restart_one(key).is_ok());
        let gate = runtime
            .start_sequence
            .as_ref()
            .and_then(|sequence| sequence.gate.as_ref())
            .expect("replacement generation remains readiness-gated");
        assert_eq!(gate.generation, 2);
        assert!(matches!(
            events_rx.try_recv(),
            Ok(SupervisorEvent::ChildRestarted {
                old_generation: 1,
                new_generation: 2,
                ..
            })
        ));

        assert!(
            runtime
                .handle_child_ready(ChildReady {
                    key,
                    instance,
                    generation: 2,
                })
                .is_ok()
        );
        assert!(matches!(
            events_rx.try_recv(),
            Ok(SupervisorEvent::ChildStarted { generation: 2, .. })
        ));
        assert!(matches!(
            events_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn stable_identity_reconciliation_reuses_static_and_closes_stale_channels() {
        let config = SupervisorBuilder::new()
            .supervisor("reused", empty_supervisor())
            .supervisor("collision", empty_supervisor())
            .build()
            .expect("valid supervisor config")
            .config
            .clone();
        let nested_channels = empty_nested_channels();
        let reused = empty_supervisor().stable_channels(true);
        let displaced = empty_supervisor().stable_channels(false);
        let orphaned = empty_supervisor().stable_channels(false);
        let reused_snapshots = reused.snapshots_rx();
        let displaced_snapshots = displaced.snapshots_rx();
        let orphaned_snapshots = orphaned.snapshots_rx();

        {
            let mut channels = nested_channels.lock().expect("nested channel map");
            channels.insert("reused".to_owned(), Arc::clone(&reused));
            channels.insert("collision".to_owned(), Arc::clone(&displaced));
            channels.insert("orphaned".to_owned(), Arc::clone(&orphaned));
        }

        let identities = reconcile_stable_identities(&config.children, &nested_channels);
        let channels = nested_channels.lock().expect("nested channel map");
        let replacement = channels.get("collision").expect("replacement identity");

        assert_eq!(channels.len(), 2);
        assert!(Arc::ptr_eq(
            channels.get("reused").expect("reused identity"),
            &reused
        ));
        assert!(Arc::ptr_eq(
            identities.get("reused").expect("reconciled identity"),
            &reused
        ));
        assert!(Arc::ptr_eq(
            identities.get("collision").expect("reconciled replacement"),
            replacement
        ));
        assert!(!Arc::ptr_eq(replacement, &displaced));
        assert!(replacement.statically_configured());
        assert!(!channels.contains_key("orphaned"));
        drop(channels);

        assert!(reused_snapshots.has_changed().is_ok());
        assert!(displaced_snapshots.has_changed().is_err());
        assert!(orphaned_snapshots.has_changed().is_err());
    }
}
