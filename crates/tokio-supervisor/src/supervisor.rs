use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{broadcast, mpsc, watch};
use tracing::{Instrument, info_span};

use crate::{
    child::{ChildDefinition, ChildKind, ChildResult},
    context::ChildContext,
    error::SupervisorError,
    event::{EventSink, SupervisorEvent},
    handle::{
        AttachedChildState, AttachedChildrenState, BoundIncarnation, NestedChannels, RootExtra,
        StableSupervisorChannels, SupervisorCommand, SupervisorHandle, empty_nested_channels,
    },
    lifecycle::LifecycleHub,
    observability::{format_path, strategy_label, supervisor_name_for_path},
    restart::{RestartIntensity, RestartPolicy},
    runtime::{SupervisorRuntime, supervision::reconcile_stable_identities},
    scope::ScopeKind,
    shutdown::{AutoShutdown, ShutdownPolicy},
    snapshot::{
        ChildMembershipView, ChildSnapshot, ChildStateView, SnapshotCell, SupervisorSnapshot,
        SupervisorStateView,
    },
    strategy::Strategy,
};

/// A configured supervisor, ready to be spawned or nested as a first-class
/// supervisor child.
///
/// Cloning a `Supervisor` produces an independent configuration that can be
/// started separately.
pub struct Supervisor {
    pub(crate) config: SupervisorConfig,
    pub(crate) channels: Arc<StableSupervisorChannels>,
    // A built declaration owns abandonment terminality until its channels are
    // handed to a parent edge. Spawned roots terminalize through this guard
    // when their owned runtime finishes.
    terminalize_on_drop: AtomicBool,
}

#[derive(Clone)]
pub(crate) struct SupervisorConfig {
    pub(crate) kind: ScopeKind,
    pub(crate) strategy: Strategy,
    pub(crate) restart_intensity: RestartIntensity,
    pub(crate) default_restart: RestartPolicy,
    pub(crate) default_shutdown: ShutdownPolicy,
    pub(crate) auto_shutdown: AutoShutdown,
    pub(crate) children: Vec<Arc<ChildDefinition>>,
    pub(crate) control_channel_capacity: usize,
    pub(crate) event_channel_capacity: usize,
}

/// Explicit connection from one nested supervisor incarnation to its parent.
#[derive(Clone)]
pub(crate) struct ParentLink {
    pub(crate) event_sink: EventSink,
    pub(crate) snapshot_cell: SnapshotCell,
    pub(crate) id: String,
    pub(crate) generation: u64,
}

struct NestedTaskOnDrop {
    abort_handle: tokio::task::AbortHandle,
    shutdown_tx: watch::Sender<bool>,
    cascade: Arc<AtomicBool>,
    armed: bool,
}

impl NestedTaskOnDrop {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for NestedTaskOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _ = self.shutdown_tx.send(true);
        if self.cascade.load(Ordering::Acquire) {
            self.abort_handle.abort();
        }
    }
}

impl ParentLink {
    pub(crate) fn publish_snapshot(&self, snapshot: SupervisorSnapshot) {
        self.snapshot_cell.forward(snapshot, self.generation);
    }

    pub(crate) fn forward_event(&self, event: SupervisorEvent) {
        self.event_sink
            .forward(self.id.clone(), self.generation, event);
    }
}

impl Supervisor {
    pub(crate) fn new(config: SupervisorConfig) -> Self {
        let channels = stable_channels_for_config(&config, false);
        Self {
            config,
            channels,
            terminalize_on_drop: AtomicBool::new(true),
        }
    }

    pub(crate) fn with_channels(
        config: SupervisorConfig,
        channels: Arc<StableSupervisorChannels>,
    ) -> Self {
        Self {
            config,
            channels,
            terminalize_on_drop: AtomicBool::new(true),
        }
    }

    /// Returns this supervisor's immutable scope kind.
    pub fn kind(&self) -> ScopeKind {
        self.config.kind
    }

    /// Returns the restart policy inherited by runtime-added children.
    pub fn default_restart_policy(&self) -> RestartPolicy {
        self.config.default_restart
    }

    /// Returns the shutdown policy inherited by runtime-added children.
    pub fn default_shutdown_policy(&self) -> ShutdownPolicy {
        self.config.default_shutdown
    }

    /// Returns this supervisor's stable control and observation handle.
    ///
    /// Before [`spawn`](Self::spawn), control operations return
    /// [`ControlError::Unavailable`](crate::ControlError), while the declared
    /// snapshot and lifecycle watches are already available.
    pub fn handle(&self) -> SupervisorHandle {
        self.channels.handle()
    }

    /// Spawns the supervisor as a background Tokio task and returns a handle
    /// for control and observation.
    pub fn spawn(self) -> SupervisorHandle {
        let channels = Arc::clone(&self.channels);
        channels.claim_edge(false);
        channels.mark_root();
        let nested_channels = channels.nested_channels();
        let attached_children = channels.attached_children();
        let initial = channels
            .take_initial_incarnation(0)
            .expect("fresh supervisor identity has initial incarnation channels");
        let initial_attached_children = initial_attached_children(&self.config, &nested_channels);
        let BoundIncarnation {
            guard: binding,
            snapshots: snapshots_tx,
            events: events_tx,
            lifecycle,
        } = channels
            .bind(
                0,
                initial.shutdown_tx.clone(),
                initial.command_tx,
                initial.done_rx.clone(),
                initial_snapshot(&self.config),
                initial_attached_children,
            )
            .expect("fresh supervisor identity binds");
        let shutdown_rx = initial.shutdown_rx;
        let command_rx = initial.command_rx;
        let done_tx = initial.done_tx;
        let done_rx = initial.done_rx;
        let task_done_tx = done_tx.clone();
        let task_channels = Arc::clone(&channels);
        let task_attached_children = Arc::clone(&attached_children);

        let join_handle = tokio::spawn(async move {
            let _binding = binding;
            let result = self
                .run_with_channels(
                    shutdown_rx,
                    events_tx,
                    lifecycle,
                    snapshots_tx,
                    task_attached_children,
                    command_rx,
                    nested_channels,
                    Vec::new(),
                    None,
                    None,
                    false,
                    task_channels.internal_handle(),
                )
                .await;
            let _ = task_done_tx.send(Some(result.clone()));
            task_channels.terminal();
            result
        });

        channels.install_root_extra(RootExtra::new(done_rx, join_handle, done_tx));
        channels.handle()
    }

    pub(crate) async fn run_as_child(
        self,
        ctx: ChildContext,
        parent_link: ParentLink,
        channels: Arc<StableSupervisorChannels>,
        path: Vec<String>,
        revivable: bool,
        abort_cascades: Arc<AtomicBool>,
    ) -> ChildResult {
        let generation = ctx.generation();
        let (shutdown_tx, shutdown_rx, command_tx, command_rx, done_tx, done_rx) =
            channels.take_initial_incarnation(generation).map_or_else(
                || {
                    let (shutdown_tx, shutdown_rx) = watch::channel(false);
                    let (command_tx, command_rx) =
                        mpsc::channel(self.config.control_channel_capacity);
                    let (done_tx, done_rx) = watch::channel(None);
                    (
                        shutdown_tx,
                        shutdown_rx,
                        command_tx,
                        command_rx,
                        done_tx,
                        done_rx,
                    )
                },
                |initial| {
                    (
                        initial.shutdown_tx,
                        initial.shutdown_rx,
                        initial.command_tx,
                        initial.command_rx,
                        initial.done_tx,
                        initial.done_rx,
                    )
                },
            );
        let nested_channels = channels.nested_channels();
        let attached_children = channels.attached_children();
        let startup_ctx = ctx.clone();
        let task_done_tx = done_tx.clone();
        let initial_snapshot = initial_snapshot(&self.config);
        // Reconcile retained stable identities before exposing the new
        // incarnation's initial attachment view. This prevents a displaced
        // dynamic same-id supervisor from leaking into the static view during
        // the interval before the runtime publishes its first snapshot.
        reconcile_stable_identities(&self.config.children, &nested_channels);
        let initial_attached_children = initial_attached_children(&self.config, &nested_channels);

        // Rebind before the runtime task can publish so observers never see
        // the previous incarnation's final snapshot through the new binding.
        let Some(BoundIncarnation {
            guard: binding,
            snapshots: snapshots_tx,
            events: events_tx,
            lifecycle,
        }) = channels.bind(
            generation,
            shutdown_tx.clone(),
            command_tx,
            done_rx,
            initial_snapshot,
            initial_attached_children,
        )
        else {
            // Reconciliation may have installed fresh descendant identities
            // after this supervisor identity was terminalized. Cascade once
            // more so none of those pre-spawn channels remain live.
            channels.terminal();
            return Ok(());
        };

        let join_handle = tokio::spawn(async move {
            let result = self
                .run_with_channels(
                    shutdown_rx,
                    events_tx,
                    lifecycle,
                    snapshots_tx,
                    attached_children,
                    command_rx,
                    nested_channels,
                    path,
                    Some(parent_link),
                    Some(startup_ctx),
                    revivable,
                    channels.internal_handle(),
                )
                .await;
            let _ = task_done_tx.send(Some(result.clone()));
            result
        });
        // Hard cascade is armed by default: aborting an ancestor runtime drops
        // its JoinSet, each nested wrapper aborts its owned runtime, and the
        // cascade continues recursively. Cooperative shutdown disarms this
        // guard after the nested runtime joins normally.
        let mut nested_task_on_drop = NestedTaskOnDrop {
            abort_handle: join_handle.abort_handle(),
            shutdown_tx: shutdown_tx.clone(),
            cascade: abort_cascades,
            armed: true,
        };
        tokio::pin!(join_handle);
        let mut shutdown_requested = false;

        let result = loop {
            tokio::select! {
                result = &mut join_handle => {
                    break match result {
                        Ok(result) => result,
                        Err(error) => Err(SupervisorError::Internal(format!(
                            "nested supervisor task failed to join: {error}"
                        ))),
                    };
                }
                _ = ctx.shutdown_token().cancelled(), if !shutdown_requested => {
                    shutdown_requested = true;
                    let _ = shutdown_tx.send(true);
                }
            }
        };
        nested_task_on_drop.disarm();

        drop(binding);
        result.map_err(|error| Box::new(error) as crate::BoxError)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_with_channels(
        self,
        shutdown_rx: watch::Receiver<bool>,
        events_tx: broadcast::Sender<SupervisorEvent>,
        lifecycle: Arc<LifecycleHub>,
        snapshots_tx: watch::Sender<SupervisorSnapshot>,
        attached_children: AttachedChildrenState,
        command_rx: mpsc::Receiver<SupervisorCommand>,
        nested_channels: NestedChannels,
        path: Vec<String>,
        parent_link: Option<ParentLink>,
        startup_ready: Option<ChildContext>,
        revivable: bool,
        own_handle: SupervisorHandle,
    ) -> Result<(), SupervisorError> {
        let supervisor_name = supervisor_name_for_path(&path).to_owned();
        let supervisor_path = format_path(&path);
        let strategy = strategy_label(self.config.strategy);
        let mut runtime = SupervisorRuntime::new(
            self.config.clone(),
            shutdown_rx,
            events_tx,
            lifecycle,
            snapshots_tx,
            attached_children,
            command_rx,
            nested_channels,
            path,
            parent_link,
            revivable,
            own_handle,
        );
        let result = runtime
            .run(startup_ready)
            .instrument(info_span!(
                "supervisor",
                supervisor_name = %supervisor_name,
                supervisor_path = %supervisor_path,
                strategy,
            ))
            .await;
        runtime.finalize_stable_channels();
        result
    }

    pub(crate) fn stable_channels(
        &self,
        statically_configured: bool,
    ) -> Arc<StableSupervisorChannels> {
        self.channels.claim_edge(statically_configured);
        self.terminalize_on_drop.store(false, Ordering::Release);
        Arc::clone(&self.channels)
    }
}

impl Clone for Supervisor {
    fn clone(&self) -> Self {
        let mut config = self.config.clone();
        config.children = self
            .config
            .children
            .iter()
            .map(|child| Arc::new((**child).clone()))
            .collect();
        Self::new(config)
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if self.terminalize_on_drop.swap(false, Ordering::AcqRel) {
            self.channels.terminal();
        }
    }
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor").finish_non_exhaustive()
    }
}

fn prepare_nested_channels(config: &SupervisorConfig) -> NestedChannels {
    let channels = empty_nested_channels();
    let mut prepared_channels = HashMap::new();

    for child in &config.children {
        if let ChildKind::Supervisor(supervisor) = &child.kind {
            let stable = supervisor.stable_channels(true);
            prepared_channels.insert(child.id.clone(), stable);
        }
    }

    *channels.lock().expect("nested channel map poisoned") = prepared_channels;
    channels
}

pub(crate) fn stable_channels_for_config(
    config: &SupervisorConfig,
    statically_configured: bool,
) -> Arc<StableSupervisorChannels> {
    let nested_channels = prepare_nested_channels(config);
    let attached_children = initial_attached_children(config, &nested_channels);
    StableSupervisorChannels::new(
        initial_snapshot(config),
        config.control_channel_capacity,
        config.event_channel_capacity,
        nested_channels,
        attached_children,
        statically_configured,
    )
}

pub(crate) fn reset_channels_for_config(
    config: &SupervisorConfig,
    channels: &StableSupervisorChannels,
) {
    let nested_channels = prepare_nested_channels(config);
    let attached_children = initial_attached_children(config, &nested_channels);
    channels.reset_declaration(
        initial_snapshot(config),
        config.control_channel_capacity,
        config.event_channel_capacity,
        nested_channels,
        attached_children,
    );
}

pub(crate) fn initial_snapshot(config: &SupervisorConfig) -> SupervisorSnapshot {
    SupervisorSnapshot {
        state: SupervisorStateView::Running,
        kind: config.kind,
        strategy: config.strategy,
        total_restarts: 0,
        lifecycle_seq: 0,
        children: config
            .children
            .iter()
            .enumerate()
            .map(|(membership_epoch, child)| ChildSnapshot {
                id: child.id.clone(),
                membership_epoch: membership_epoch as u64,
                generation: 0,
                started: false,
                startup_aborted: false,
                state: ChildStateView::Starting,
                membership: ChildMembershipView::Active,
                last_exit: None,
                restart_count: 0,
                next_restart_in: None,
                supervisor: None,
            })
            .collect(),
    }
}

fn initial_attached_children(
    config: &SupervisorConfig,
    nested_channels: &NestedChannels,
) -> Vec<AttachedChildState> {
    let nested_channels = nested_channels.lock().expect("nested channel map poisoned");
    config
        .children
        .iter()
        .enumerate()
        .map(|(membership_epoch, child)| AttachedChildState {
            identity: crate::AttachedChildIdentity {
                id: child.id.clone(),
                membership_epoch: membership_epoch as u64,
                generation: 0,
            },
            attachment: child.attachment.clone(),
            supervisor: nested_channels
                .get(&child.id)
                .map(StableSupervisorChannels::internal_handle),
        })
        .collect()
}
