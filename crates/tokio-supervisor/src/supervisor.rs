use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, watch};
use tracing::{Instrument, info_span};

use crate::{
    StartMode,
    child::{ChildDefinition, ChildKind, ChildResult},
    context::ChildContext,
    error::SupervisorError,
    event::{EventSink, SupervisorEvent},
    handle::{
        NestedChannels, StableSupervisorChannels, SupervisorCommand, SupervisorHandle,
        SupervisorHandleInit, empty_nested_channels,
    },
    observability::{format_path, strategy_label, supervisor_name_for_path},
    restart::RestartIntensity,
    runtime::SupervisorRuntime,
    shutdown::AutoShutdown,
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
#[derive(Clone)]
pub struct Supervisor {
    pub(crate) config: SupervisorConfig,
}

#[derive(Clone)]
pub(crate) struct SupervisorConfig {
    pub(crate) strategy: Strategy,
    pub(crate) start_mode: StartMode,
    pub(crate) restart_intensity: RestartIntensity,
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
        Self { config }
    }

    /// Spawns the supervisor as a background Tokio task and returns a handle
    /// for control and observation.
    pub fn spawn(self) -> SupervisorHandle {
        let nested_channels = prepare_nested_channels(&self.config);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (command_tx, command_rx) = mpsc::channel(self.config.control_channel_capacity);
        let (done_tx, done_rx) = watch::channel(None);
        let (events_tx, _) = broadcast::channel(self.config.event_channel_capacity);
        let (snapshots_tx, snapshots_rx) = watch::channel(initial_snapshot(&self.config));
        let task_done_tx = done_tx.clone();
        let task_events_tx = events_tx.clone();
        let task_snapshots_tx = snapshots_tx.clone();
        let task_nested_channels = nested_channels.clone();

        let join_handle = tokio::spawn(async move {
            let result = self
                .run_with_channels(
                    shutdown_rx,
                    task_events_tx,
                    task_snapshots_tx,
                    command_rx,
                    task_nested_channels,
                    Vec::new(),
                    None,
                    None,
                    false,
                )
                .await;
            let _ = task_done_tx.send(Some(result.clone()));
            result
        });

        SupervisorHandle::new(SupervisorHandleInit {
            shutdown_tx,
            command_tx,
            nested_channels,
            done_tx,
            done_rx,
            events_tx,
            snapshots_rx,
            join_handle,
        })
    }

    pub(crate) async fn run_as_child(
        self,
        ctx: ChildContext,
        parent_link: ParentLink,
        channels: Arc<StableSupervisorChannels>,
        path: Vec<String>,
        revivable: bool,
    ) -> ChildResult {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (command_tx, command_rx) = mpsc::channel(self.config.control_channel_capacity);
        let (done_tx, done_rx) = watch::channel(None);
        let events_tx = channels.events();
        let snapshots_tx = channels.snapshots();
        let nested_channels = channels.nested_channels();
        let generation = ctx.generation();
        let startup_ctx = ctx.clone();
        let task_done_tx = done_tx.clone();
        let initial_snapshot = initial_snapshot(&self.config);

        // Rebind before the runtime task can publish so observers never see
        // the previous incarnation's final snapshot through the new binding.
        let binding = channels.bind(
            generation,
            shutdown_tx.clone(),
            command_tx,
            done_rx,
            initial_snapshot,
        );

        let join_handle = tokio::spawn(async move {
            let result = self
                .run_with_channels(
                    shutdown_rx,
                    events_tx,
                    snapshots_tx,
                    command_rx,
                    nested_channels,
                    path,
                    Some(parent_link),
                    Some(startup_ctx),
                    revivable,
                )
                .await;
            let _ = task_done_tx.send(Some(result.clone()));
            result
        });

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

        drop(binding);
        result.map_err(|error| Box::new(error) as crate::BoxError)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_with_channels(
        self,
        shutdown_rx: watch::Receiver<bool>,
        events_tx: broadcast::Sender<SupervisorEvent>,
        snapshots_tx: watch::Sender<SupervisorSnapshot>,
        command_rx: mpsc::Receiver<SupervisorCommand>,
        nested_channels: NestedChannels,
        path: Vec<String>,
        parent_link: Option<ParentLink>,
        startup_ready: Option<ChildContext>,
        revivable: bool,
    ) -> Result<(), SupervisorError> {
        let supervisor_name = supervisor_name_for_path(&path).to_owned();
        let supervisor_path = format_path(&path);
        let strategy = strategy_label(self.config.strategy);
        let mut runtime = SupervisorRuntime::new(
            self.config,
            shutdown_rx,
            events_tx,
            snapshots_tx,
            command_rx,
            nested_channels,
            path,
            parent_link,
            revivable,
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
        let nested_channels = prepare_nested_channels(&self.config);
        StableSupervisorChannels::new(
            initial_snapshot(&self.config),
            self.config.event_channel_capacity,
            nested_channels,
            statically_configured,
        )
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

pub(crate) fn initial_snapshot(config: &SupervisorConfig) -> SupervisorSnapshot {
    SupervisorSnapshot {
        state: SupervisorStateView::Running,
        strategy: config.strategy,
        total_restarts: 0,
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
