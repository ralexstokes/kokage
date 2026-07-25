use std::{
    collections::HashMap,
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
    builder::StartMode,
    child::{ChildDefinition, ChildKind, ChildReadiness, OpaqueAttachment, SupervisorSpec},
    context::ChildReady,
    error::{ControlError, SupervisorError},
    event::{ExitStatusView, NestedEventNotification, SupervisorEvent},
    handle::{
        AttachedChildState, AttachedChildrenState, NestedChannels, StableSupervisorChannels,
        SupervisorCommand,
    },
    observability::{SupervisorObservability, format_child_path},
    restart::{RestartIntensity, RestartPolicy},
    shutdown::AutoShutdown,
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
enum JoinInterest {
    None,
    WhenNonEmpty,
    Required,
}

#[derive(Clone, Copy)]
struct WakeOptions {
    commands: bool,
    nested_snapshots: bool,
    nested_events: bool,
    readiness: bool,
    readiness_first: bool,
    joins: JoinInterest,
    deadline: Option<Instant>,
}

impl WakeOptions {
    fn main_loop() -> Self {
        Self {
            commands: true,
            nested_snapshots: true,
            nested_events: true,
            readiness: true,
            readiness_first: false,
            joins: JoinInterest::WhenNonEmpty,
            deadline: None,
        }
    }

    fn readiness() -> Self {
        Self {
            commands: false,
            nested_snapshots: true,
            nested_events: true,
            readiness: true,
            readiness_first: true,
            joins: JoinInterest::WhenNonEmpty,
            deadline: None,
        }
    }

    fn restart_delay(deadline: Instant) -> Self {
        Self {
            commands: true,
            nested_snapshots: false,
            nested_events: false,
            readiness: false,
            readiness_first: false,
            joins: JoinInterest::None,
            deadline: Some(deadline),
        }
    }

    fn child_removal(deadline: Instant) -> Self {
        Self {
            commands: false,
            nested_snapshots: false,
            nested_events: false,
            readiness: false,
            readiness_first: false,
            joins: JoinInterest::Required,
            deadline: Some(deadline),
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
/// different slab occupants at the same key (e.g. after a child is removed and
/// a new one is inserted at the recycled key). Combined with `generation`
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
    pub(crate) start_mode: StartMode,
    pub(crate) auto_shutdown: AutoShutdown,
    pub(crate) default_restart_intensity: RestartIntensity,
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
    /// Parent token whose children are the per-child tokens. Cancelling this
    /// token cancels all children at once (used in shutdown and `OneForAll`
    /// restarts).
    pub(crate) group_token: CancellationToken,
    pub(crate) join_set: JoinSet<ChildEnvelope>,
    pub(crate) children: Slab<ChildEntry>,
    pub(crate) children_by_id: HashMap<String, ChildKey>,
    pub(crate) child_order: Vec<ChildKey>,
    pub(crate) live_tasks: usize,
    pub(crate) next_child_instance: u64,
    pub(crate) events: broadcast::Sender<SupervisorEvent>,
    pub(crate) snapshots: watch::Sender<SupervisorSnapshot>,
    pub(crate) attached_children: AttachedChildrenState,
    pub(crate) shutdown_rx: watch::Receiver<bool>,
    pub(crate) command_rx: mpsc::Receiver<SupervisorCommand>,
    pub(crate) nested_channels: NestedChannels,
    pub(crate) nested_event_tx: mpsc::Sender<NestedEventNotification>,
    pub(crate) nested_event_rx: mpsc::Receiver<NestedEventNotification>,
    pub(crate) nested_snapshot_tx: mpsc::UnboundedSender<NestedSnapshotNotification>,
    pub(crate) nested_snapshot_rx: mpsc::UnboundedReceiver<NestedSnapshotNotification>,
    pub(crate) ready_tx: mpsc::UnboundedSender<ChildReady>,
    pub(crate) ready_rx: mpsc::UnboundedReceiver<ChildReady>,
    pub(crate) commands_open: bool,
    pub(crate) task_map: HashMap<Id, TaskMeta>,
    pub(crate) restart_epoch: u64,
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
        snapshots: watch::Sender<SupervisorSnapshot>,
        attached_children: AttachedChildrenState,
        command_rx: mpsc::Receiver<SupervisorCommand>,
        nested_channels: NestedChannels,
        path_prefix: Vec<String>,
        parent_link: Option<ParentLink>,
        revivable: bool,
    ) -> Self {
        let default_restart_intensity = config.restart_intensity;
        let start_mode = config.start_mode;
        let observability = SupervisorObservability::new(path_prefix.clone(), config.strategy);
        let mut children = Slab::with_capacity(config.children.len());
        let mut children_by_id = HashMap::with_capacity(config.children.len());
        let mut child_order = Vec::with_capacity(config.children.len());
        let mut next_child_instance = 0u64;
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
            let key = children.insert(ChildEntry::new(
                id.clone(),
                formatted_path,
                spec,
                child_nested_channels,
                default_restart_intensity,
                next_child_instance,
            ));
            next_child_instance = next_child_instance.saturating_add(1);
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
                start_mode,
                auto_shutdown: config.auto_shutdown,
                default_restart_intensity,
                path_prefix,
                observability,
                parent_link,
                revivable,
            },
            state: SupervisorState::Running,
            group_token: CancellationToken::new(),
            join_set: JoinSet::new(),
            children,
            children_by_id,
            child_order,
            live_tasks: 0,
            next_child_instance,
            events,
            snapshots,
            attached_children,
            shutdown_rx,
            command_rx,
            nested_channels,
            nested_event_tx,
            nested_event_rx,
            nested_snapshot_tx,
            nested_snapshot_rx,
            ready_tx,
            ready_rx,
            commands_open: true,
            task_map: HashMap::new(),
            restart_epoch: 0,
            total_restarts,
        }
    }

    pub(crate) async fn run(
        &mut self,
        startup_ready: Option<crate::context::ChildContext>,
    ) -> Result<(), SupervisorError> {
        match self.run_until_exit(startup_ready).await {
            Ok(()) => Ok(()),
            Err(ExitReason::Shutdown) => self.shutdown_all().await,
            Err(ExitReason::Failure(error)) => Err(error),
        }
    }

    async fn run_until_exit(
        &mut self,
        startup_ready: Option<crate::context::ChildContext>,
    ) -> RuntimeResult<()> {
        self.publish_snapshot();
        self.send_event(SupervisorEvent::SupervisorStarted);
        let initial_children = self.child_order.clone();
        let initial_instances: Vec<_> = initial_children
            .iter()
            .filter_map(|&key| self.children.get(key).map(|entry| (key, entry.instance)))
            .collect();
        let startup_completed = self.start_children(initial_children.clone()).await?;
        if let Some(startup_ready) = startup_ready {
            if startup_completed && self.wait_until_children_ready(&initial_instances).await? {
                startup_ready.mark_ready();
            } else {
                let error = SupervisorError::StartupAborted(
                    "nested supervisor child failed before startup completed".to_owned(),
                );
                let _ = self.shutdown_all().await;
                return Err(ExitReason::Failure(error));
            }
        }

        loop {
            match self.next_wake(WakeOptions::main_loop()).await {
                Wake::Shutdown => return Err(ExitReason::Shutdown),
                Wake::Command(command) => match command {
                    Some(command) => self.handle_command(command).await?,
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
                        self.handle_child_ready(ready);
                    }
                }
                Wake::Joined(maybe) => {
                    if let Some(joined) = maybe {
                        self.handle_joined_child(joined).await?;
                    }
                }
                Wake::Deadline => unreachable!("main loop has no deadline"),
            }
        }
    }

    /// Waits for the next enabled runtime input. Shutdown is always enabled
    /// and always wins when multiple inputs are ready.
    async fn next_wake(&mut self, options: WakeOptions) -> Wake {
        loop {
            if options.readiness_first {
                if self.shutdown_rx.has_changed().is_err() || *self.shutdown_rx.borrow() {
                    return Wake::Shutdown;
                }
                match self.ready_rx.try_recv() {
                    Ok(ready) => return Wake::Ready(Some(ready)),
                    Err(mpsc::error::TryRecvError::Disconnected) => return Wake::Ready(None),
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }
            }

            let wait_for_join = match options.joins {
                JoinInterest::None => false,
                JoinInterest::WhenNonEmpty => !self.join_set.is_empty(),
                JoinInterest::Required => true,
            };
            let deadline = options.deadline.unwrap_or_else(Instant::now);
            let wake = tokio::select! {
                biased;
                changed = self.shutdown_rx.changed() => {
                    self.shutdown_requested(changed).then_some(Wake::Shutdown)
                }
                command = self.command_rx.recv(), if options.commands && self.commands_open => {
                    Some(Wake::Command(command))
                }
                update = self.nested_snapshot_rx.recv(), if options.nested_snapshots => {
                    Some(Wake::NestedSnapshot(update))
                }
                event = self.nested_event_rx.recv(), if options.nested_events => {
                    Some(Wake::NestedEvent(event))
                }
                ready = self.ready_rx.recv(), if options.readiness => {
                    Some(Wake::Ready(ready))
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

    async fn start_children(&mut self, keys: Vec<ChildKey>) -> RuntimeResult<bool> {
        self.respawn_sequence(keys, false).await
    }

    async fn respawn_sequence(
        &mut self,
        keys: Vec<ChildKey>,
        emit_restart_events: bool,
    ) -> RuntimeResult<bool> {
        let restart_epoch = self.restart_epoch;
        for key in keys {
            if emit_restart_events {
                match self.group_respawn_disposition(key) {
                    GroupRespawnDisposition::Respawn => {}
                    GroupRespawnDisposition::Skip => continue,
                    GroupRespawnDisposition::Finalize { startup_aborted } => {
                        self.finalize_skipped_group_respawn(key, startup_aborted);
                        continue;
                    }
                }
            }
            let SpawnOutcome::Spawned {
                ready,
                old_generation,
                new_generation,
            } = Box::pin(self.spawn_child_for_start(key)).await?
            else {
                continue;
            };
            if self.restart_epoch != restart_epoch {
                return Ok(true);
            }
            if emit_restart_events && let Some(old_generation) = old_generation {
                self.send_restart_event(key, old_generation, new_generation);
            }
            if !ready {
                if self.children.contains(key) {
                    return Ok(false);
                }
                continue;
            }
        }
        Ok(true)
    }

    async fn spawn_child_for_start(&mut self, key: ChildKey) -> RuntimeResult<SpawnOutcome> {
        let Some(entry) = self.children.get(key) else {
            return Ok(SpawnOutcome::Skipped);
        };
        if entry.membership != MembershipState::Active
            || entry.runtime.state != RuntimeChildState::Stopped
        {
            return Ok(SpawnOutcome::Skipped);
        }
        let readiness_gated = entry.runtime.definition.readiness == ChildReadiness::Explicit;
        let (old_generation, new_generation) = self.spawn_child(key)?;
        let ready = if self.meta.start_mode == StartMode::Sequential && readiness_gated {
            Box::pin(self.wait_until_child_ready(key)).await?
        } else {
            true
        };
        Ok(SpawnOutcome::Spawned {
            ready,
            old_generation,
            new_generation,
        })
    }

    async fn wait_until_child_ready(&mut self, key: ChildKey) -> RuntimeResult<bool> {
        let Some(instance) = self.children.get(key).map(|entry| entry.instance) else {
            return Ok(false);
        };
        loop {
            let Some(entry) = self.children.get(key) else {
                return Ok(false);
            };
            if entry.instance != instance {
                return Ok(false);
            }
            match entry.runtime.state {
                RuntimeChildState::Running => return Ok(true),
                RuntimeChildState::Stopped => return Ok(false),
                RuntimeChildState::Starting | RuntimeChildState::Stopping => {}
            }

            match self.next_wake(WakeOptions::readiness()).await {
                Wake::Shutdown => return Err(ExitReason::Shutdown),
                Wake::Ready(ready) => {
                    if let Some(ready) = ready {
                        self.handle_child_ready(ready);
                    }
                }
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
                Wake::Joined(maybe) => {
                    if let Some(joined) = maybe {
                        self.handle_joined_child(joined).await?;
                    }
                }
                Wake::Command(_) | Wake::Deadline => {
                    unreachable!("readiness wait enables only readiness inputs")
                }
            }
        }
    }

    async fn wait_until_children_ready(
        &mut self,
        children: &[(ChildKey, u64)],
    ) -> RuntimeResult<bool> {
        for &(key, instance) in children {
            let Some(entry) = self.children.get(key) else {
                continue;
            };
            if entry.instance != instance {
                continue;
            }
            if entry.runtime.has_reported_ready {
                continue;
            }
            if !Box::pin(self.wait_until_child_ready(key)).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn handle_child_ready(&mut self, ready: ChildReady) {
        let Some(entry) = self.children.get_mut(ready.key) else {
            return;
        };
        if entry.instance != ready.instance
            || entry.runtime.generation != ready.generation
            || entry.membership != MembershipState::Active
            || entry.runtime.state != RuntimeChildState::Starting
        {
            return;
        }
        entry.runtime.state = RuntimeChildState::Running;
        entry.runtime.has_reported_ready = true;
        let id = entry.id.clone();
        self.send_event(SupervisorEvent::ChildStarted {
            id,
            generation: ready.generation,
        });
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

    fn finalize_skipped_group_respawn(&mut self, key: ChildKey, startup_aborted: bool) {
        if startup_aborted {
            self.children[key].runtime.startup_aborted = true;
            self.publish_snapshot();
        }
        // Skipped by the group respawn and never restarted afterwards; if
        // this supervisor is the root, that judgment is final.
        if self.children[key].runtime.definition.remove_on_exit {
            self.finalize_removed_child(key);
        } else {
            self.mark_child_terminal(key);
        }
    }

    async fn handle_command(&mut self, command: SupervisorCommand) -> RuntimeResult<()> {
        match command {
            SupervisorCommand::AddChild { child, reply } => {
                complete_command(reply, self.add_child(child).await)
            }
            SupervisorCommand::RemoveChild { id, reply } => {
                complete_command(reply, self.remove_child(id).await)
            }
            SupervisorCommand::AddSupervisor {
                id,
                supervisor,
                reply,
            } => complete_command(reply, self.add_supervisor(id, *supervisor).await),
        }
    }

    async fn add_child(&mut self, child: crate::child::ChildSpec) -> CommandResult<u64> {
        if self.state == SupervisorState::Stopping {
            return Err(ControlError::SupervisorStopping.into());
        }

        if child.id().is_empty() {
            return Err(ControlError::InvalidConfig("child id must not be empty").into());
        }

        if let Some(restart_intensity) = child.restart_intensity_override() {
            restart_intensity
                .validate()
                .map_err(|err| map_build_error_to_control(child.id(), err))?;
        }
        if child.is_significant() && matches!(child.restart_policy(), RestartPolicy::Always) {
            return Err(ControlError::InvalidConfig(
                "significant children cannot use RestartPolicy::Always",
            )
            .into());
        }
        if child.is_significant() && matches!(self.meta.auto_shutdown, AutoShutdown::Never) {
            return Err(ControlError::InvalidConfig(
                "significant children require automatic shutdown",
            )
            .into());
        }

        let id = child.id().to_owned();
        if self.children_by_id.contains_key(&id) {
            return Err(ControlError::DuplicateChildId(id).into());
        }

        let formatted_path = format_child_path(&self.meta.path_prefix, &id);
        let definition = child.inner;
        let membership_epoch = self.next_child_instance;
        let key = self.children.insert(ChildEntry::new(
            id.clone(),
            formatted_path,
            definition,
            None,
            self.meta.default_restart_intensity,
            membership_epoch,
        ));
        self.next_child_instance = self.next_child_instance.saturating_add(1);
        self.children_by_id.insert(id.clone(), key);
        self.child_order.push(key);

        self.start_children(vec![key]).await?;

        Ok(membership_epoch)
    }

    async fn add_supervisor(
        &mut self,
        id: String,
        supervisor: SupervisorSpec,
    ) -> CommandResult<u64> {
        if self.state == SupervisorState::Stopping {
            return Err(ControlError::SupervisorStopping.into());
        }
        if id.is_empty() {
            return Err(ControlError::InvalidConfig("child id must not be empty").into());
        }
        if let Some(intensity) = supervisor.restart_intensity {
            intensity
                .validate()
                .map_err(|error| map_build_error_to_control(&id, error))?;
        }
        if supervisor.significant && matches!(supervisor.restart, RestartPolicy::Always) {
            return Err(ControlError::InvalidConfig(
                "significant children cannot use RestartPolicy::Always",
            )
            .into());
        }
        if supervisor.significant && matches!(self.meta.auto_shutdown, AutoShutdown::Never) {
            return Err(ControlError::InvalidConfig(
                "significant children require automatic shutdown",
            )
            .into());
        }
        if self.children_by_id.contains_key(&id) {
            return Err(ControlError::DuplicateChildId(id).into());
        }

        let stable = supervisor.supervisor.stable_channels(false);
        let definition = Arc::new(ChildDefinition::supervisor(id.clone(), supervisor));
        let formatted_path = format_child_path(&self.meta.path_prefix, &id);
        let membership_epoch = self.next_child_instance;
        let key = self.children.insert(ChildEntry::new(
            id.clone(),
            formatted_path,
            definition,
            Some(Arc::clone(&stable)),
            self.meta.default_restart_intensity,
            membership_epoch,
        ));
        self.next_child_instance = self.next_child_instance.saturating_add(1);
        self.children_by_id.insert(id.clone(), key);
        self.child_order.push(key);
        self.nested_channels
            .lock()
            .expect("nested channel map poisoned")
            .insert(id.clone(), stable);

        self.start_children(vec![key]).await?;

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

    async fn remove_child(&mut self, id: String) -> CommandResult<()> {
        if self.state == SupervisorState::Stopping {
            return Err(ControlError::SupervisorStopping.into());
        }

        let Some(&key) = self.children_by_id.get(&id) else {
            return Err(ControlError::UnknownChildId(id).into());
        };

        if self.children[key].membership == MembershipState::Removing {
            return Err(ControlError::ChildRemovalInProgress(id).into());
        }

        let (mode, grace, active) = {
            let entry = &mut self.children[key];
            entry.membership = MembershipState::Removing;
            let active = entry.runtime.state.is_active();
            if active {
                entry.runtime.state = RuntimeChildState::Stopping;
            }
            (
                entry.runtime.definition.shutdown_policy.mode,
                entry.runtime.definition.shutdown_policy.grace,
                active,
            )
        };

        self.publish_snapshot();

        if !active {
            self.finalize_removed_child(key);
            return Ok(());
        }

        match mode {
            crate::shutdown::ShutdownMode::Abort => {
                self.abort_and_detach_child(key).await?;
                Ok(())
            }
            crate::shutdown::ShutdownMode::CooperativeStrict => {
                self.cancel_child(key);
                self.await_child_removal(key, Instant::now() + grace, true)
                    .await
            }
            crate::shutdown::ShutdownMode::CooperativeThenAbort => {
                self.cancel_child(key);
                self.await_child_removal(key, Instant::now() + grace, false)
                    .await
            }
        }
    }

    async fn await_child_removal(
        &mut self,
        key: ChildKey,
        deadline: Instant,
        timeout_is_error: bool,
    ) -> CommandResult<()> {
        let child_id = self.child_id(key).ok_or_else(|| {
            ControlError::Internal("missing child id while removing child".to_owned())
        })?;
        let started_at = StdInstant::now();
        let mut removal_error: Option<ControlError> = None;

        loop {
            if !self.children.contains(key)
                || self.children[key].membership == MembershipState::Removed
            {
                self.meta.observability.record_shutdown_duration(
                    "remove_child",
                    started_at.elapsed(),
                    Some(&child_id),
                );
                return removal_error.map_or(Ok(()), |error| Err(error.into()));
            }

            match self.next_wake(WakeOptions::child_removal(deadline)).await {
                Wake::Shutdown => return Err(ExitReason::Shutdown.into()),
                Wake::Joined(maybe) => {
                    self.handle_join_during_control(maybe).await?;
                }
                Wake::Deadline => {
                    self.meta
                        .observability
                        .record_shutdown_timeout("remove_child", Some(&child_id));
                    if timeout_is_error {
                        removal_error = Some(ControlError::ShutdownTimedOut(child_id.clone()));
                    }
                    self.abort_and_detach_child(key).await?;
                    self.meta.observability.record_shutdown_duration(
                        "remove_child",
                        started_at.elapsed(),
                        Some(&child_id),
                    );
                    return removal_error.map_or(Ok(()), |error| Err(error.into()));
                }
                Wake::Command(_)
                | Wake::NestedSnapshot(_)
                | Wake::NestedEvent(_)
                | Wake::Ready(_) => {
                    unreachable!("child removal enables only joins and its deadline")
                }
            }
        }
    }

    async fn handle_join_during_control(
        &mut self,
        maybe: Option<JoinedChild>,
    ) -> CommandResult<()> {
        let Some(joined) = maybe else {
            return Err(ControlError::Internal(
                "supervisor join set drained before child removal completed".to_owned(),
            )
            .into());
        };

        self.handle_joined_child(joined).await?;

        Ok(())
    }

    fn cancel_child(&mut self, key: ChildKey) {
        self.children[key].runtime.completion.mark_cancelled();
        if let Some(token) = self.children[key].runtime.active_token.as_ref() {
            token.cancel();
        }
    }

    fn abort_child(&mut self, key: ChildKey) {
        self.children[key].runtime.completion.mark_cancelled();
        if let Some(abort_handle) = self.children[key].runtime.abort_handle.as_ref() {
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

    fn child_id(&self, key: ChildKey) -> Option<String> {
        self.children.get(key).map(|entry| entry.id.clone())
    }

    pub(crate) fn child_path(&self, key: ChildKey) -> Vec<String> {
        let mut path = self.meta.path_prefix.clone();
        path.push(self.children[key].id.clone());
        path
    }

    fn finalize_removed_child(&mut self, key: ChildKey) {
        if !self.children.contains(key) {
            return;
        }

        let had_live_task = self.children[key].runtime.abort_handle.is_some();
        let mut entry = self.children.remove(key);
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
        self.send_event(SupervisorEvent::ChildRemoved { id });
    }

    async fn handle_joined_child(
        &mut self,
        joined: Result<(Id, ChildEnvelope), JoinError>,
    ) -> RuntimeResult<()> {
        let Some(classified) = self.consume_joined_child(joined)? else {
            return Ok(());
        };
        self.dispatch_exit(classified).await
    }

    async fn dispatch_exit(&mut self, classified: ClassifiedExit) -> RuntimeResult<()> {
        self.record_exit(classified.key, classified.generation, &classified.status);
        self.apply_exit_policy(classified).await
    }

    async fn apply_exit_policy(&mut self, classified: ClassifiedExit) -> RuntimeResult<()> {
        self.apply_exit_policy_inner(classified, true).await
    }

    async fn apply_drained_completion_policy(
        &mut self,
        classified: ClassifiedExit,
    ) -> RuntimeResult<()> {
        self.apply_exit_policy_inner(classified, false).await
    }

    async fn apply_exit_policy_inner(
        &mut self,
        classified: ClassifiedExit,
        allow_restart: bool,
    ) -> RuntimeResult<()> {
        if self.state != SupervisorState::Running {
            return Ok(());
        }

        if self.children[classified.key].membership == MembershipState::Removing {
            self.finalize_removed_child(classified.key);
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
            match self.meta.strategy {
                Strategy::OneForOne => self.handle_one_for_one_restart(classified.key).await?,
                Strategy::OneForAll => self.handle_one_for_all_restart(classified.key).await?,
                Strategy::RestForOne => self.handle_rest_for_one_restart(classified.key).await?,
            }
        } else if allow_restart {
            if !self.children[classified.key].runtime.has_reported_ready {
                self.children[classified.key].runtime.startup_aborted = true;
                self.publish_snapshot();
            }
            if self.children[classified.key]
                .runtime
                .definition
                .remove_on_exit
            {
                self.finalize_removed_child(classified.key);
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
        self.send_event(SupervisorEvent::ChildExited {
            id,
            generation,
            status: status.view(),
        });
    }

    async fn handle_one_for_one_restart(&mut self, key: ChildKey) -> RuntimeResult<()> {
        let Some(permit) = self.begin_restart(key).await? else {
            return Ok(());
        };
        let (old_generation, new_generation) = self.spawn_child(key)?;
        self.send_restart_event(
            key,
            old_generation.unwrap_or(permit.previous_generation),
            new_generation,
        );
        Ok(())
    }

    async fn handle_one_for_all_restart(&mut self, failing_key: ChildKey) -> RuntimeResult<()> {
        self.restart_group(failing_key, true).await?;
        Ok(())
    }

    async fn handle_rest_for_one_restart(&mut self, failing_key: ChildKey) -> RuntimeResult<()> {
        let deferred = self.restart_group(failing_key, false).await?;
        for classified in deferred {
            // A deferred child may already have been respawned (or removed) by
            // an earlier deferred dispatch's suffix restart; its recorded exit
            // is then stale and must not be applied to the fresh generation.
            if !self.current_child_matches(
                classified.key,
                classified.instance,
                classified.generation,
            ) {
                continue;
            }
            Box::pin(self.apply_exit_policy(classified)).await?;
        }
        Ok(())
    }

    async fn begin_restart(&mut self, key: ChildKey) -> RuntimeResult<Option<RestartPermit>> {
        let restart_instance = self.children[key].instance;
        let previous_generation = self.children[key].runtime.generation;
        let delay = self.schedule_restart(key)?;
        self.send_event(SupervisorEvent::ChildRestartScheduled {
            id: self.children[key].id.clone(),
            generation: previous_generation,
            delay,
        });
        self.wait_for_restart_delay(delay).await?;
        let Some(entry) = self.children.get(key) else {
            return Ok(None);
        };
        if entry.instance != restart_instance || entry.membership != MembershipState::Active {
            return Ok(None);
        }
        Ok(Some(RestartPermit {
            previous_generation,
        }))
    }

    async fn restart_group(
        &mut self,
        failing_key: ChildKey,
        fresh_group_token: bool,
    ) -> RuntimeResult<Vec<ClassifiedExit>> {
        if self.begin_restart(failing_key).await?.is_none() {
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
            Box::pin(self.apply_drained_completion_policy(classified)).await?;
        }
        if fresh_group_token {
            self.group_token = CancellationToken::new();
        }
        self.restart_epoch = self.restart_epoch.saturating_add(1);
        let _ = self.respawn_sequence(keys, true).await?;
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

    async fn wait_for_restart_delay(&mut self, delay: Duration) -> RuntimeResult<()> {
        let deadline = Instant::now() + delay;
        // Even an immediate restart yields once so commands triggered by the
        // restart-scheduled event can enter the queue. The biased wake below
        // drains those commands before accepting the already-ready deadline.
        if delay.is_zero() {
            tokio::task::yield_now().await;
        }

        loop {
            match self.next_wake(WakeOptions::restart_delay(deadline)).await {
                Wake::Shutdown => return Err(ExitReason::Shutdown),
                Wake::Command(Some(command)) => {
                    Box::pin(self.handle_command(command)).await?;
                }
                Wake::Command(None) => self.commands_open = false,
                Wake::Deadline => return Ok(()),
                Wake::NestedSnapshot(_)
                | Wake::NestedEvent(_)
                | Wake::Ready(_)
                | Wake::Joined(_) => {
                    unreachable!("restart delay enables only commands and its deadline")
                }
            }
        }
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
        let _ = self.snapshots.send_replace(snapshot.clone());
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
                    .map(|channels| channels.handle()),
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
                    RuntimeChildState::Starting => ChildStateView::Starting,
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
            strategy: self.meta.strategy,
            total_restarts: self.total_restarts,
            children,
        }
    }

    async fn drain_ready_joins(&mut self) -> RuntimeResult<()> {
        loop {
            match tokio::time::timeout(Duration::ZERO, self.join_set.join_next_with_id()).await {
                Ok(Some(joined)) => {
                    self.handle_joined_child(joined).await?;
                }
                Ok(None) | Err(_) => return Ok(()),
            }
        }
    }

    async fn abort_and_detach_child(&mut self, key: ChildKey) -> RuntimeResult<()> {
        self.abort_child(key);
        tokio::task::yield_now().await;
        self.drain_ready_joins().await?;
        if self.state != SupervisorState::Running {
            return Ok(());
        }
        self.finalize_removed_child_if_present(key);
        Ok(())
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

    fn finalize_removed_child_if_present(&mut self, key: ChildKey) {
        if self.children.contains(key) {
            self.finalize_removed_child(key);
        }
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

enum SpawnOutcome {
    Skipped,
    Spawned {
        ready: bool,
        old_generation: Option<u64>,
        new_generation: u64,
    },
}

enum GroupRespawnDisposition {
    Respawn,
    Skip,
    Finalize { startup_aborted: bool },
}

struct RestartPermit {
    previous_generation: u64,
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
        Supervisor, SupervisorBuilder,
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
        let config = empty_supervisor().config;
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
            snapshots_tx,
            attached_children_state(None, Vec::new()),
            command_rx,
            empty_nested_channels(),
            Vec::new(),
            None,
            false,
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
            runtime.next_wake(WakeOptions::readiness()).await,
            Wake::Ready(Some(ChildReady { key: 1, .. }))
        ));
    }

    #[test]
    fn stable_identity_reconciliation_reuses_static_and_closes_stale_channels() {
        let config = SupervisorBuilder::new()
            .supervisor("reused", empty_supervisor())
            .supervisor("collision", empty_supervisor())
            .build()
            .expect("valid supervisor config")
            .config;
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
