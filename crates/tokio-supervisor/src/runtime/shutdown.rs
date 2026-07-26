use std::{collections::HashSet, time::Instant as StdInstant};

use slab::Slab;
use tokio::time::{Instant, sleep_until};
use tracing::{Instrument, info_span};

use crate::{
    error::SupervisorError,
    event::SupervisorEvent,
    runtime::{
        child_runtime::RuntimeChildState,
        supervision::{
            ChildEntry, ChildKey, ClassifiedExit, DrainReason, MembershipState, SupervisorState,
        },
    },
    scope::ScopeKind,
    shutdown::ShutdownMode,
};

use super::supervision::SupervisorRuntime;

#[derive(Clone, Copy)]
enum DrainScope<'a> {
    All,
    Subset(&'a HashSet<ChildKey>),
}

impl DrainScope<'_> {
    fn contains(self, key: ChildKey) -> bool {
        match self {
            Self::All => true,
            Self::Subset(keys) => keys.contains(&key),
        }
    }

    fn is_drained(self, runtime: &SupervisorRuntime) -> bool {
        match self {
            Self::All => runtime.live_tasks == 0,
            Self::Subset(keys) => !keys.iter().any(|&key| {
                runtime.children.get(key).is_some_and(|child| {
                    child.membership != MembershipState::Removed && child.runtime.state.is_active()
                })
            }),
        }
    }
}

impl SupervisorRuntime {
    pub(crate) async fn shutdown_all(&mut self) -> Result<(), SupervisorError> {
        let span = info_span!(
            "shutdown",
            supervisor_name = %self.meta.observability.supervisor_name(),
            supervisor_path = %self.meta.observability.supervisor_path(),
        );

        async {
            self.state = SupervisorState::Stopping;
            self.stopping_token.cancel();
            self.send_event(SupervisorEvent::SupervisorStopping);
            if self.meta.kind == ScopeKind::Ordered {
                self.drain_children_ordered(DrainReason::Shutdown, DrainScope::All)
                    .await?;
            } else {
                self.cancel_running_children(DrainScope::All);
                self.drain_children(DrainReason::Shutdown, DrainScope::All)
                    .await?;
            }
            self.finish();
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn drain_for_group_restart(
        &mut self,
    ) -> Result<Vec<ClassifiedExit>, SupervisorError> {
        if self.meta.kind == ScopeKind::Ordered {
            self.drain_children_ordered(DrainReason::GroupRestart, DrainScope::All)
                .await
        } else {
            self.cancel_running_children(DrainScope::All);
            self.drain_children(DrainReason::GroupRestart, DrainScope::All)
                .await
        }
    }

    pub(crate) async fn drain_for_rest_for_one_restart(
        &mut self,
        keys: &[ChildKey],
    ) -> Result<Vec<ClassifiedExit>, SupervisorError> {
        let keys: HashSet<_> = keys.iter().copied().collect();
        if self.meta.kind == ScopeKind::Ordered {
            self.drain_children_ordered(DrainReason::RestForOneRestart, DrainScope::Subset(&keys))
                .await
        } else {
            self.cancel_running_children(DrainScope::Subset(&keys));
            self.drain_children(DrainReason::RestForOneRestart, DrainScope::Subset(&keys))
                .await
        }
    }

    fn cancel_running_children(&mut self, scope: DrainScope<'_>) {
        for &key in self.child_order.iter().rev() {
            if !scope.contains(key) {
                continue;
            }
            let Some(child) = self.children.get_mut(key) else {
                continue;
            };

            if matches!(
                child.runtime.state,
                RuntimeChildState::Running | RuntimeChildState::Starting
            ) {
                child.runtime.completion.mark_cancelled();
                child.runtime.state = RuntimeChildState::Stopping;
                if matches!(scope, DrainScope::Subset(_))
                    && let Some(token) = child.runtime.active_token.as_ref()
                {
                    token.cancel();
                }
            }
        }
        if matches!(scope, DrainScope::All) {
            self.group_token.cancel();
        }
    }

    /// Drains one ordered child at a time, in reverse declaration order.
    /// Each cooperative child receives its own complete grace window. The
    /// next child is not cancelled until the current task has joined or has
    /// been issued an abort, so cooperative dependents are fully gone before
    /// their dependencies begin teardown without letting a non-yielding task
    /// stall the supervisor indefinitely.
    async fn drain_children_ordered(
        &mut self,
        reason: DrainReason,
        scope: DrainScope<'_>,
    ) -> Result<Vec<ClassifiedExit>, SupervisorError> {
        if matches!(reason, DrainReason::Shutdown) {
            self.command_rx.close();
        }
        let started_at = StdInstant::now();
        let keys: Vec<_> = self
            .child_order
            .iter()
            .rev()
            .copied()
            .filter(|&key| scope.contains(key))
            .collect();
        let mut deferred = Vec::new();
        let mut timed_out = Vec::new();
        let mut remaining = Vec::new();

        for key in keys {
            let Some(child) = self.children.get(key) else {
                continue;
            };
            if child.membership == MembershipState::Removed || !child.runtime.state.is_active() {
                continue;
            }

            let id = child.id.clone();
            let policy = child.runtime.definition.shutdown_policy;
            self.children[key].runtime.state = RuntimeChildState::Stopping;
            let aborted_immediately = matches!(policy.mode, ShutdownMode::Abort);
            match policy.mode {
                ShutdownMode::Abort => self.abort_child(key),
                ShutdownMode::CooperativeStrict | ShutdownMode::CooperativeThenAbort => {
                    self.cancel_child(key)
                }
            }

            let expired = if matches!(policy.mode, ShutdownMode::Abort) {
                false
            } else {
                self.wait_for_ordered_child(
                    key,
                    Some(Instant::now() + policy.grace),
                    scope,
                    &mut deferred,
                )
                .await?
            };

            if expired {
                if matches!(reason, DrainReason::Shutdown) {
                    self.meta
                        .observability
                        .record_shutdown_timeout("shutdown", Some(&id));
                }
                if matches!(policy.mode, ShutdownMode::CooperativeStrict) {
                    timed_out.push(id.clone());
                }
                self.abort_child(key);
            }

            if aborted_immediately || expired {
                // Give a normal Tokio abort one scheduling turn to complete so
                // its exit remains ordered. A future that does not reach a
                // poll boundary must not retain the cursor indefinitely.
                tokio::task::yield_now().await;
                let still_active = self
                    .wait_for_ordered_child(
                        key,
                        Some(Instant::now() + std::time::Duration::from_millis(1)),
                        scope,
                        &mut deferred,
                    )
                    .await?;
                if still_active {
                    remaining.push(id);
                }
            }
        }

        self.record_drain_duration(reason, started_at);
        if !timed_out.is_empty() {
            return Err(SupervisorError::ShutdownTimedOut(timed_out.join(", ")));
        }
        if !remaining.is_empty() && !matches!(reason, DrainReason::Shutdown) {
            return Err(SupervisorError::ShutdownTimedOut(remaining.join(", ")));
        }
        Ok(deferred)
    }

    /// Returns `true` when `deadline` expires while `key` is still active.
    async fn wait_for_ordered_child(
        &mut self,
        key: ChildKey,
        deadline: Option<Instant>,
        scope: DrainScope<'_>,
        deferred: &mut Vec<ClassifiedExit>,
    ) -> Result<bool, SupervisorError> {
        loop {
            if !self.children.get(key).is_some_and(|child| {
                child.membership != MembershipState::Removed && child.runtime.state.is_active()
            }) {
                return Ok(false);
            }

            if let Some(deadline) = deadline {
                tokio::select! {
                    joined = self.join_set.join_next_with_id() => {
                        let Some(joined) = joined else { return Ok(false); };
                        self.handle_join_for_scope(joined, scope, Some(key), deferred)?;
                    }
                    _ = sleep_until(deadline) => return Ok(true),
                }
            } else {
                let Some(joined) = self.join_set.join_next_with_id().await else {
                    return Ok(false);
                };
                self.handle_join_for_scope(joined, scope, Some(key), deferred)?;
            }
        }
    }

    async fn drain_children(
        &mut self,
        reason: DrainReason,
        scope: DrainScope<'_>,
    ) -> Result<Vec<ClassifiedExit>, SupervisorError> {
        if matches!(reason, DrainReason::Shutdown) {
            self.command_rx.close();
        }
        let started_at = StdInstant::now();
        let mut deferred = Vec::new();
        let mut max_grace: Option<std::time::Duration> = None;

        for (key, child) in self.children.iter() {
            if !scope.contains(key)
                || child.membership == MembershipState::Removed
                || !child.runtime.state.is_active()
            {
                continue;
            }

            let grace = child.runtime.definition.shutdown_policy.grace;
            match child.runtime.definition.shutdown_policy.mode {
                ShutdownMode::Abort => {}
                ShutdownMode::CooperativeStrict | ShutdownMode::CooperativeThenAbort => {
                    max_grace = Some(max_grace.map_or(grace, |current| current.max(grace)));
                }
            }
        }

        abort_matching_children(&self.children, |key, child| {
            scope.contains(key)
                && matches!(
                    child.runtime.definition.shutdown_policy.mode,
                    ShutdownMode::Abort
                )
        });
        tokio::task::yield_now().await;
        self.drain_ready_joins_for_scope(scope, &mut deferred)
            .await?;
        if scope.is_drained(self) {
            self.record_drain_duration(reason, started_at);
            return Ok(deferred);
        }

        if let Some(grace) = max_grace {
            let deadline = Instant::now() + grace;
            while !scope.is_drained(self) {
                tokio::select! {
                    maybe = self.join_set.join_next_with_id() => {
                        let Some(joined) = maybe else { break; };
                        self.handle_join_for_scope(joined, scope, None, &mut deferred)?;
                    }
                    _ = sleep_until(deadline) => break,
                }
            }

            if !scope.is_drained(self) && matches!(reason, DrainReason::Shutdown) {
                self.meta
                    .observability
                    .record_shutdown_timeout("shutdown", None);
            }
        }

        let timed_out = collect_child_names(&self.children, |key, child| {
            scope.contains(key)
                && child.membership != MembershipState::Removed
                && (matches!(reason, DrainReason::Shutdown)
                    || child.membership != MembershipState::Removing)
                && child.runtime.state.is_active()
                && matches!(
                    child.runtime.definition.shutdown_policy.mode,
                    ShutdownMode::CooperativeStrict
                )
        });
        let remaining = active_task_names(&self.children, scope);
        if !remaining.is_empty() {
            abort_matching_children(&self.children, |key, _| scope.contains(key));
            tokio::task::yield_now().await;
            self.drain_ready_joins_for_scope(scope, &mut deferred)
                .await?;
        }

        let remaining = active_task_names(&self.children, scope);
        self.record_drain_duration(reason, started_at);
        if !timed_out.is_empty() {
            return Err(SupervisorError::ShutdownTimedOut(timed_out));
        }
        if !remaining.is_empty() && !matches!(reason, DrainReason::Shutdown) {
            return Err(SupervisorError::ShutdownTimedOut(remaining));
        }
        Ok(deferred)
    }

    async fn drain_ready_joins_for_scope(
        &mut self,
        scope: DrainScope<'_>,
        deferred: &mut Vec<ClassifiedExit>,
    ) -> Result<(), SupervisorError> {
        loop {
            match tokio::time::timeout(std::time::Duration::ZERO, self.join_set.join_next_with_id())
                .await
            {
                Ok(Some(joined)) => self.handle_join_for_scope(joined, scope, None, deferred)?,
                Ok(None) | Err(_) => return Ok(()),
            }
        }
    }

    fn handle_join_for_scope(
        &mut self,
        joined: Result<
            (tokio::task::Id, super::supervision::ChildEnvelope),
            tokio::task::JoinError,
        >,
        scope: DrainScope<'_>,
        ordered_cursor: Option<ChildKey>,
        deferred: &mut Vec<ClassifiedExit>,
    ) -> Result<(), SupervisorError> {
        let Some(classified) = self.consume_joined_child(joined)? else {
            return Ok(());
        };
        // Record the exit immediately even when dispatch is deferred: the
        // entry must not look Running once its join is consumed, or a nested
        // drain that includes this key would wait on a join that never comes.
        self.record_exit(classified.key, classified.generation, &classified.status);
        if self.children[classified.key].membership == MembershipState::Removing {
            self.finalize_removed_child(classified.key, true);
            return Ok(());
        }
        let naturally_completed = matches!(classified.status, super::exit::ExitStatus::Completed)
            && self.children[classified.key].runtime.completion.is_clean();
        let non_cursor_ordered_exit = ordered_cursor.is_some_and(|cursor| cursor != classified.key);
        if !scope.contains(classified.key) || naturally_completed || non_cursor_ordered_exit {
            deferred.push(classified);
        }
        Ok(())
    }

    fn record_drain_duration(&self, reason: DrainReason, started_at: StdInstant) {
        self.meta.observability.record_shutdown_duration(
            shutdown_operation(reason),
            started_at.elapsed(),
            None,
        );
    }
}

fn abort_matching_children(
    children: &Slab<ChildEntry>,
    predicate: impl Fn(ChildKey, &ChildEntry) -> bool,
) {
    for (key, child) in children.iter() {
        if child.membership != MembershipState::Removed
            && child.runtime.state.is_active()
            && predicate(key, child)
            && let Some(abort_handle) = child.runtime.abort_handle.as_ref()
        {
            child
                .runtime
                .nested_abort_cascades
                .store(true, std::sync::atomic::Ordering::Release);
            abort_handle.abort();
        }
    }
}

fn active_task_names(children: &Slab<ChildEntry>, scope: DrainScope<'_>) -> String {
    collect_child_names(children, |key, child| {
        scope.contains(key)
            && child.membership != MembershipState::Removed
            && child.runtime.state.is_active()
    })
}

fn collect_child_names(
    children: &Slab<ChildEntry>,
    predicate: impl Fn(ChildKey, &ChildEntry) -> bool,
) -> String {
    children
        .iter()
        .filter(|(key, child)| predicate(*key, child))
        .map(|(_, child)| child.id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn shutdown_operation(reason: DrainReason) -> &'static str {
    match reason {
        DrainReason::Shutdown => "shutdown",
        DrainReason::GroupRestart => "group_restart",
        DrainReason::RestForOneRestart => "rest_for_one_restart",
    }
}
