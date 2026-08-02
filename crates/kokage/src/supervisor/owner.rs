use std::{
    collections::HashMap,
    sync::{
        Arc, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{mpsc, watch};
use tracing::{Instrument, info_span};

use crate::{
    actor::ExitResult,
    supervisor::{
        builder::{DynamicSupervisorBuilder, OrderedSupervisorBuilder},
        child::{ChildDefinition, ChildKind},
        context::TaskContext,
        error::SupervisorError,
        handle::{
            AttachedChildState, AttachedChildrenState, BoundIncarnation, NestedChannels, RootExtra,
            StableSupervisorChannels, SupervisorCommand, SupervisorHandle, empty_nested_channels,
        },
        lifecycle::{LifecycleHub, LifecycleTreeSink},
        observability::{format_path, strategy_label, supervisor_name_for_path},
        restart::RestartPolicy,
        runtime::{SupervisorRuntime, supervision::reconcile_stable_identities},
        scope::ScopeKind,
        shutdown::Shutdown,
        snapshot::{
            ChildMembershipView, ChildSnapshot, ChildStateView, SnapshotCell, SupervisorSnapshot,
            SupervisorStateView,
        },
        strategy::Strategy,
    },
};

/// A single-use configured supervisor, ready to be spawned or nested as a
/// first-class supervisor child.
///
/// A `Supervisor` owns the stable identity behind [`handle`](Self::handle),
/// reserved when its builder was created. Moving the declaration into
/// [`spawn`](Self::spawn) or nesting it under a parent scope transfers that
/// identity. Clone the handle, not the declaration, when multiple observers
/// or controllers need to address it.
pub struct Supervisor {
    pub(crate) config: SupervisorConfig,
    pub(crate) channels: Arc<StableSupervisorChannels>,
    // A built declaration owns abandonment terminality until its channels are
    // handed to a parent edge. Spawned roots terminalize through this guard
    // when their owned runtime finishes.
    terminalize_on_drop: AtomicBool,
}

/// Owning guard for a spawned root supervisor.
///
/// A running supervisor has exactly one owner. Dropping this value requests a
/// graceful shutdown, while any [`SupervisorHandle`] values obtained through
/// [`handle`](Self::handle) remain non-owning and may be dropped freely.
/// Retain the owner for as long as the root supervisor should keep running.
#[must_use = "dropping the running supervisor requests graceful shutdown"]
pub struct RunningSupervisor {
    handle: SupervisorHandle,
}

impl RunningSupervisor {
    /// Returns a non-owning handle for control and observation.
    pub fn handle(&self) -> SupervisorHandle {
        self.handle.clone()
    }

    /// Requests a graceful shutdown and waits for the supervisor to stop.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.handle.shutdown_and_wait().await
    }

    /// Waits for the supervisor to stop.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.handle.wait().await
    }
}

impl Drop for RunningSupervisor {
    fn drop(&mut self) {
        self.handle.shutdown();
    }
}

impl std::fmt::Debug for RunningSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningSupervisor").finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct SupervisorConfig {
    pub(crate) kind: ScopeKind,
    pub(crate) strategy: Strategy,
    pub(crate) default_child_restart: RestartPolicy,
    pub(crate) default_child_shutdown: Shutdown,
    pub(crate) children: Vec<Arc<ChildDefinition>>,
    pub(crate) control_channel_capacity: usize,
}

/// Explicit connection from one nested supervisor incarnation to its parent.
#[derive(Clone)]
pub(crate) struct ParentLink {
    pub(crate) lifecycle_tree: LifecycleTreeSink,
    pub(crate) snapshot_cell: SnapshotCell,
    pub(crate) id: String,
    pub(crate) lineage: u64,
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
}

impl Supervisor {
    /// Starts an ordered-supervisor declaration.
    pub fn ordered() -> OrderedSupervisorBuilder {
        OrderedSupervisorBuilder::new()
    }

    /// Starts an empty dynamic-supervisor declaration.
    pub fn dynamic() -> DynamicSupervisorBuilder {
        DynamicSupervisorBuilder::new()
    }

    pub(crate) fn new(config: SupervisorConfig) -> Self {
        let channels = stable_channels_for_config(&config);
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

    // Test-only access preserves the pre-spawn contract: control operations
    // are unavailable before `spawn`, while declared snapshots and lifecycle
    // watches can already be observed.
    #[cfg(test)]
    pub fn handle(&self) -> SupervisorHandle {
        self.channels.handle()
    }

    /// Spawns the supervisor as a background Tokio task and returns its owner.
    ///
    /// Dropping the returned [`RunningSupervisor`] requests graceful shutdown.
    /// Handles obtained from it are non-owning.
    pub fn spawn(self) -> RunningSupervisor {
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
            binding_epoch,
            snapshots: snapshots_tx,
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
                    lifecycle,
                    snapshots_tx,
                    task_attached_children,
                    binding_epoch,
                    command_rx,
                    nested_channels,
                    Vec::new(),
                    None,
                    None,
                    false,
                    task_channels.handle(),
                )
                .await;
            let _ = task_done_tx.send(Some(result.clone()));
            task_channels.terminal();
            result
        });

        channels.install_root_extra(RootExtra::new(done_rx, join_handle, done_tx));
        RunningSupervisor {
            handle: channels.handle(),
        }
    }

    pub(crate) async fn run_as_child(
        self,
        ctx: TaskContext,
        parent_link: ParentLink,
        channels: Arc<StableSupervisorChannels>,
        path: Vec<String>,
        revivable: bool,
        abort_cascades: Arc<AtomicBool>,
    ) -> ExitResult {
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

        // Reconcile and rebind as one ownership transaction. This prevents a
        // displaced dynamic same-id supervisor from leaking into the static
        // view, and prevents an old pending removal from retiring an identity
        // after the replacement has selected it.
        let Some(BoundIncarnation {
            guard: binding,
            binding_epoch,
            snapshots: snapshots_tx,
            lifecycle,
        }) = channels.bind_prepared(
            generation,
            shutdown_tx.clone(),
            command_tx,
            done_rx,
            initial_snapshot,
            || {
                reconcile_stable_identities(&self.config.children, &nested_channels);
                initial_attached_children(&self.config, &nested_channels)
            },
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
                    lifecycle,
                    snapshots_tx,
                    attached_children,
                    binding_epoch,
                    command_rx,
                    nested_channels,
                    path,
                    Some(parent_link),
                    Some(startup_ctx),
                    revivable,
                    channels.handle(),
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
        let mut abort_requested = false;

        let result = loop {
            tokio::select! {
                result = &mut join_handle => {
                    break match result {
                        Ok(result) => result,
                        Err(_error) if abort_requested => {
                            Err(SupervisorError::ShutdownTimedOut(ctx.id().to_owned()))
                        }
                        Err(error) => Err(SupervisorError::Internal(format!(
                            "nested supervisor task failed to join: {error}"
                        ))),
                    };
                }
                _ = ctx.shutdown_token().cancelled(), if !shutdown_requested => {
                    shutdown_requested = true;
                    let _ = shutdown_tx.send(true);
                }
                _ = ctx.abort_token().cancelled(), if !abort_requested => {
                    abort_requested = true;
                    join_handle.abort();
                }
            }
        };
        nested_task_on_drop.disarm();

        // The inner task normally publishes its own result. When this wrapper
        // hard-aborts that task, it instead owns the synthesized timeout (and
        // likewise any join failure), so publish the wrapper's final result
        // before retiring the binding. Late stable-identity waiters then see
        // the same outcome as the parent that joined this child.
        let _ = done_tx.send(Some(result.clone()));
        drop(binding);
        result.map_err(|error| Box::new(error) as crate::supervisor::BoxError)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_with_channels(
        self,
        shutdown_rx: watch::Receiver<bool>,
        lifecycle: Arc<LifecycleHub>,
        snapshots_tx: watch::Sender<SupervisorSnapshot>,
        attached_children: AttachedChildrenState,
        binding_epoch: u64,
        command_rx: mpsc::Receiver<SupervisorCommand>,
        nested_channels: NestedChannels,
        path: Vec<String>,
        parent_link: Option<ParentLink>,
        startup_ready: Option<TaskContext>,
        revivable: bool,
        own_handle: SupervisorHandle,
    ) -> Result<(), SupervisorError> {
        let supervisor_name = supervisor_name_for_path(&path).to_owned();
        let supervisor_path = format_path(&path);
        let strategy = strategy_label(self.config.strategy);
        let mut runtime = SupervisorRuntime::new(
            self.config.clone(),
            shutdown_rx,
            lifecycle,
            snapshots_tx,
            attached_children,
            binding_epoch,
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

impl Supervisor {
    /// Re-instantiates private runtime configuration for a new incarnation.
    ///
    /// This is deliberately not a public declaration-cloning API. Public
    /// declarations own one stable identity; the runtime needs only a fresh
    /// executable value when restarting a nested supervisor.
    pub(crate) fn instantiate_runtime(&self) -> Self {
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

    *channels.lock().unwrap_or_else(PoisonError::into_inner) = prepared_channels;
    channels
}

pub(crate) fn stable_channels_for_config(
    config: &SupervisorConfig,
) -> Arc<StableSupervisorChannels> {
    let nested_channels = prepare_nested_channels(config);
    let attached_children = initial_attached_children(config, &nested_channels);
    StableSupervisorChannels::new(
        initial_snapshot(config),
        config.control_channel_capacity,
        nested_channels,
        attached_children,
    )
}

/// Republishes a reserved identity's declared view after a builder mutation.
pub(crate) fn refresh_declaration_for_config(
    config: &SupervisorConfig,
    channels: &StableSupervisorChannels,
) {
    let nested_channels = prepare_nested_channels(config);
    let attached_children = initial_attached_children(config, &nested_channels);
    channels.reset_declared_view(initial_snapshot(config), nested_channels, attached_children);
}

/// Applies a finished configuration to a reserved identity, once, at build.
pub(crate) fn reset_channels_for_config(
    config: &SupervisorConfig,
    channels: &StableSupervisorChannels,
) {
    refresh_declaration_for_config(config, channels);
    channels.reset_declared_capacities(config.control_channel_capacity);
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
            .map(|(lineage, child)| ChildSnapshot {
                id: child.id.clone(),
                lineage: lineage as u64,
                generation: 0,
                state: ChildStateView::Starting {
                    previous_exit: None,
                },
                membership: ChildMembershipView::Active,
                restart_count: 0,
                restart_policy: child.restart,
                remove_when_done: child.remove_when_done,
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
    let nested_channels = nested_channels
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    config
        .children
        .iter()
        .enumerate()
        .map(|(lineage, child)| AttachedChildState {
            identity: crate::supervisor::attachment::AttachedChildIdentity {
                id: child.id.clone(),
                lineage: lineage as u64,
                generation: 0,
            },
            attachment: child.attachment.clone(),
            supervisor: nested_channels
                .get(&child.id)
                .map(StableSupervisorChannels::handle),
        })
        .collect()
}
