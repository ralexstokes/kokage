use std::sync::{Arc, atomic::Ordering};

use crate::supervisor::{
    CancellationToken,
    child::{ChildKind, ChildReadiness},
    context::{ChildContext, ChildReady, ReadySignal},
    error::SupervisorError,
    event::RuntimeEvent,
    handle::StableSupervisorChannels,
    owner::ParentLink,
    runtime::{
        child_runtime::{CompletionFlag, RuntimeChildState},
        supervision::{ChildEnvelope, SupervisorRuntime, TaskMeta},
    },
    snapshot::{NestedSnapshotState, SnapshotCell},
};
use tracing::{Instrument, info_span};

struct SpawnPlan {
    child_id: String,
    generation: u64,
    old_generation: Option<u64>,
    kind: ChildKind,
    ctx: ChildContext,
    lineage: u64,
    snapshot_state: Option<NestedSnapshotState>,
    nested_channels: Option<Arc<StableSupervisorChannels>>,
    completion: CompletionFlag,
    nested_abort_cascades: Arc<std::sync::atomic::AtomicBool>,
}

impl SupervisorRuntime {
    pub(crate) fn spawn_child(
        &mut self,
        key: usize,
    ) -> Result<(Option<u64>, u64), SupervisorError> {
        let SpawnPlan {
            child_id,
            generation,
            old_generation,
            kind,
            ctx,
            lineage,
            snapshot_state,
            nested_channels,
            completion,
            nested_abort_cascades,
        } = {
            let entry = self.children.get_mut(key).ok_or_else(|| {
                SupervisorError::Internal(format!("missing child slot for key {key}"))
            })?;
            let child = &mut entry.runtime;
            let lineage = entry.lineage;

            let old_generation = if child.has_started {
                Some(child.generation)
            } else {
                None
            };
            if child.has_started {
                child.generation = child.generation.saturating_add(1);
            }

            let generation = child.generation;
            child
                .restart_tracker
                .record_spawn(tokio::time::Instant::now());
            let child_token = self.group_token.child_token();
            let abort_token = CancellationToken::new();
            child.active_token = Some(child_token.clone());
            child.active_abort_token = Some(abort_token.clone());
            child.shutdown_timed_out = false;
            child.state = RuntimeChildState::Starting;
            child.has_reported_ready = false;
            child.startup_aborted = false;
            child.next_restart_deadline = None;
            let completion = CompletionFlag::pending();
            child.completion = completion.clone();
            child.nested_abort_cascades.store(true, Ordering::Release);
            let nested_abort_cascades = Arc::clone(&child.nested_abort_cascades);
            entry.nested_snapshot = None;
            let snapshot_state = if matches!(&child.definition.kind, ChildKind::Supervisor(_)) {
                let state = NestedSnapshotState::default();
                entry.nested_snapshot_state = Some(state.clone());
                Some(state)
            } else {
                entry.nested_snapshot_state = None;
                None
            };

            let child_id = entry.id.clone();
            let ctx = ChildContext::new(
                child_id.clone(),
                generation,
                child_token,
                abort_token,
                self.own_handle.clone(),
                (child.definition.readiness == ChildReadiness::Explicit).then(|| {
                    ReadySignal::new(
                        self.ready_tx.clone(),
                        ChildReady {
                            key,
                            lineage,
                            generation,
                        },
                    )
                }),
            );
            let kind = child.definition.kind.clone();
            let nested_channels = entry.nested_channels.clone();

            SpawnPlan {
                child_id,
                generation,
                old_generation,
                kind,
                ctx,
                lineage,
                snapshot_state,
                nested_channels,
                completion,
                nested_abort_cascades,
            }
        };
        let child_path_segments = self.child_path(key);
        // A nested supervisor can be revived after this incarnation ends if
        // its own policy permits a restart, or if this supervisor can itself
        // be reincarnated — but reincarnation respawns only the *static*
        // children; a dynamically added child is orphaned instead, so
        // ancestor revivability does not propagate across a dynamic edge.
        // The child's terminality judgments about its own children are final
        // only when neither condition holds.
        let statically_configured = nested_channels
            .as_ref()
            .is_some_and(|channels| channels.statically_configured());
        let child_revivable = (self.meta.revivable && statically_configured)
            || !self.children[key].runtime.definition.restart.is_never();
        let nested_run = if matches!(&kind, ChildKind::Supervisor(_)) {
            let channels = nested_channels.ok_or_else(|| {
                SupervisorError::Internal(format!(
                    "missing stable channels for nested supervisor {child_id}"
                ))
            })?;
            let snapshot_state = snapshot_state.ok_or_else(|| {
                SupervisorError::Internal(format!(
                    "missing snapshot cell for nested supervisor {child_id}"
                ))
            })?;
            Some((
                ParentLink {
                    lifecycle_tree: self.lifecycle_tree.clone(),
                    snapshot_cell: SnapshotCell::new(
                        self.nested_snapshot_tx.clone(),
                        snapshot_state,
                        key,
                        lineage,
                    ),
                    id: child_id.clone(),
                    lineage,
                    generation,
                },
                channels,
                child_path_segments,
            ))
        } else {
            None
        };
        let child_path = self.meta.observability.child_path(&child_id);
        let supervisor_name = self.meta.observability.supervisor_name().to_owned();
        let supervisor_path = self.meta.observability.supervisor_path().to_owned();
        let child_span = info_span!(
            "child",
            supervisor_name = %supervisor_name,
            supervisor_path = %supervisor_path,
            child_id = %child_id,
            child_path = %child_path,
            generation,
        );

        let abort_handle = self.join_set.spawn(
            async move {
                let result = match kind {
                    ChildKind::Task(factory) => factory.make(ctx).await,
                    ChildKind::Supervisor(supervisor) => {
                        let (parent_link, channels, child_path_segments) =
                            nested_run.expect("nested run state validated before spawn");
                        supervisor
                            .run_as_child(
                                ctx,
                                parent_link,
                                channels,
                                child_path_segments,
                                child_revivable,
                                nested_abort_cascades,
                            )
                            .await
                    }
                };
                if result.is_ok() {
                    completion.mark_clean();
                }
                ChildEnvelope { result }
            }
            .instrument(child_span),
        );
        let task_id = abort_handle.id();

        {
            let entry = self.children.get_mut(key).ok_or_else(|| {
                SupervisorError::Internal(format!("missing child slot for key {key}"))
            })?;
            let child = &mut entry.runtime;
            child.has_started = true;
            child.state = if child.definition.readiness == ChildReadiness::Immediate {
                child.has_reported_ready = true;
                RuntimeChildState::Running
            } else {
                RuntimeChildState::Starting
            };
            child.abort_handle = Some(abort_handle);
        }
        self.live_tasks = self.live_tasks.saturating_add(1);
        self.task_map.insert(
            task_id,
            TaskMeta {
                key,
                lineage,
                generation,
            },
        );
        if self.children[key].runtime.state == RuntimeChildState::Running {
            self.send_lifecycle(
                key,
                crate::supervisor::lifecycle::ChildLifecycleEventKind::Started { generation },
            );
            self.send_event(RuntimeEvent::ChildStarted {
                id: child_id,
                generation,
            });
        } else {
            self.publish_snapshot();
        }

        Ok((old_generation, generation))
    }
}
