//! Tracking children that represent finite work.
//!
//! A supervisor normally runs until it is told to stop. Pipeline and batch
//! subtrees invert that: some children have a natural completion point, and the
//! scope's job is done once they reach it. This module expresses that as a
//! reduction over [`watch_lifecycle`](crate::supervisor::SupervisorHandle::watch_lifecycle)
//! rather than as supervisor configuration, so the completion rule lives with
//! the code that cares about it instead of in the control loop.

use std::collections::{HashMap, HashSet};

use crate::supervisor::{
    CancellationToken, CompletionOnDrop, ExitStatus, Guard, LifecycleEvent, LifecycleEventKind,
    ScopeKind,
    handle::SupervisorHandle,
    snapshot::{ChildMembershipView, ChildSnapshot, ChildStateView, SupervisorSnapshot},
};

/// Error returned when a direct-child completion condition cannot be satisfied.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CompletionError {
    /// Future-member mode was requested for an ordered scope.
    #[error("scope has ordered membership")]
    NotDynamic,
    /// The child id is not present in the scope's declared/current membership.
    #[error("unknown child `{child_id}`")]
    UnknownChild {
        /// Child id supplied to the wait.
        child_id: String,
    },
    /// The scope became terminal before every awaited child completed.
    #[error("scope closed before the children completed")]
    ScopeClosed,
}

struct CompletionOperation {
    handle: SupervisorHandle,
    set: CompletionSet,
    watch: crate::supervisor::LifecycleWatch,
    baseline: u64,
}

impl CompletionOperation {
    fn new<I, S>(
        handle: SupervisorHandle,
        ids: I,
        allow_future_members: bool,
    ) -> Result<Self, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if allow_future_members && handle.kind() != ScopeKind::Dynamic {
            return Err(CompletionError::NotDynamic);
        }

        // Register before taking the snapshot so no transition can fall
        // between the state baseline and the stream.
        let watch = handle.watch_lifecycle().direct_children();
        let snapshot = handle.snapshot();
        let mut set = CompletionSet::new(ids);
        let baseline = set.initialize(&snapshot, allow_future_members)?;
        Ok(Self {
            handle,
            set,
            watch,
            baseline,
        })
    }

    async fn wait(mut self) -> Result<(), CompletionError> {
        loop {
            if self.set.is_complete() {
                // `Exited` is emitted before its immediately following
                // restart-scheduled transition. Recheck state before completing so
                // `RestartPolicy::always()` cannot expose that transient stop as
                // finished work.
                self.baseline = self.set.realign(&self.handle.snapshot());
                if self.set.is_complete() {
                    return Ok(());
                }
            }
            let Some(event) = self.watch.next().await else {
                return Err(CompletionError::ScopeClosed);
            };
            if matches!(event.kind, LifecycleEventKind::Lagged { .. }) {
                // A dropped prefix may have contained transitions for awaited
                // children, so edge-derived state has to be rebuilt from state.
                self.baseline = self.set.realign(&self.handle.snapshot());
            } else if event.scope_path.is_empty()
                && event.kind.seq().is_some_and(|seq| seq > self.baseline)
            {
                let needs_realign = matches!(
                    &event.kind,
                    LifecycleEventKind::ChildAdded { .. } | LifecycleEventKind::ChildExited { .. }
                );
                self.set.apply(&event);
                if needs_realign {
                    // The snapshot is published before this exit event and knows
                    // the restart policy and whether a replacement is pending.
                    // Realigning here avoids treating an Always-policy clean exit
                    // as complete even transiently.
                    self.baseline = self.set.realign(&self.handle.snapshot());
                }
            }
        }
    }
}

impl SupervisorHandle {
    pub(crate) async fn wait_for_children<I, S>(&self, ids: I) -> Result<(), CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CompletionOperation::new(self.clone(), ids, false)?
            .wait()
            .await
    }

    pub(crate) async fn wait_for_future_children<I, S>(&self, ids: I) -> Result<(), CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CompletionOperation::new(self.clone(), ids, true)?
            .wait()
            .await
    }

    pub(crate) fn shutdown_when_children_complete<I, S>(
        &self,
        ids: I,
    ) -> Result<Guard, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.shutdown_when_children_complete_inner(ids, false)
    }

    pub(crate) fn shutdown_when_future_children_complete<I, S>(
        &self,
        ids: I,
    ) -> Result<Guard, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.shutdown_when_children_complete_inner(ids, true)
    }

    fn shutdown_when_children_complete_inner<I, S>(
        &self,
        ids: I,
        allow_future_members: bool,
    ) -> Result<Guard, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let operation = CompletionOperation::new(self.clone(), ids, allow_future_members)?;
        let child_ids = operation.set.awaited.clone();
        let scope_kind = self.kind();
        let handle = self.clone();
        let cancellation = CancellationToken::new();
        let (finished, finished_on_drop) = CompletionOnDrop::armed();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let _finished_on_drop = finished_on_drop;
            let result = tokio::select! {
                biased;
                () = task_cancellation.cancelled() => return,
                result = operation.wait() => result,
            };
            match result {
                Ok(()) => handle.shutdown(),
                Err(CompletionError::ScopeClosed) => {}
                Err(error) => tracing::warn!(
                    %error,
                    ?scope_kind,
                    ?child_ids,
                    "completion-triggered shutdown watch failed"
                ),
            }
        });

        std::mem::drop(task);
        Ok(Guard::from_tokens(cancellation, finished))
    }
}

/// The reduction behind the direct completion operations.
struct CompletionSet {
    /// The children being awaited, in the order the caller named them.
    awaited: Vec<String>,
    /// Awaited children currently in a completed state.
    satisfied: HashSet<String>,
    /// Awaited children this set has ever observed as a membership.
    ///
    /// Absence from a snapshot is ambiguous on its own: a child may have been
    /// removed, or may not have been added yet. Only a child seen at least
    /// once can be treated as removed.
    seen: HashSet<String>,
    /// Newest membership lineage observed for each awaited child id.
    ///
    /// A replacement supervisor incarnation can begin before its displaced
    /// predecessor finishes shutting down. Events from that predecessor are
    /// still delivered, but their older lineage must not alter the replacement
    /// membership's completion state.
    latest_lineages: HashMap<String, u64>,
    /// Restart policy of the newest snapshot-aligned membership for each id.
    restart_policies: HashMap<String, crate::supervisor::RestartPolicy>,
}

impl CompletionSet {
    fn new<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            awaited: ids.into_iter().map(Into::into).collect(),
            satisfied: HashSet::new(),
            seen: HashSet::new(),
            latest_lineages: HashMap::new(),
            restart_policies: HashMap::new(),
        }
    }

    fn is_complete(&self) -> bool {
        self.awaited.iter().all(|id| self.satisfied.contains(id))
    }

    fn awaits(&self, id: &str) -> bool {
        self.awaited.iter().any(|awaited| awaited == id)
    }

    /// Validates strict membership and aligns state from one snapshot.
    fn initialize(
        &mut self,
        snapshot: &SupervisorSnapshot,
        allow_future_members: bool,
    ) -> Result<u64, CompletionError> {
        if !allow_future_members
            && let Some(child_id) = self.awaited.iter().find(|id| snapshot.child(id).is_none())
        {
            return Err(CompletionError::UnknownChild {
                child_id: child_id.clone(),
            });
        }
        Ok(self.realign(snapshot))
    }

    fn apply(&mut self, event: &LifecycleEvent) {
        let (child_id, lineage, transition) = match &event.kind {
            LifecycleEventKind::ChildAdded {
                child_id, lineage, ..
            }
            | LifecycleEventKind::ChildStarted {
                child_id, lineage, ..
            } => (child_id, *lineage, CompletionTransition::Running),
            LifecycleEventKind::ChildExited {
                child_id,
                lineage,
                exit,
                ..
            } => (child_id, *lineage, CompletionTransition::Exited(exit)),
            LifecycleEventKind::ChildRemoved {
                child_id, lineage, ..
            } => (child_id, *lineage, CompletionTransition::Removed),
            _ => return,
        };
        if !self.awaits(child_id) {
            return;
        }
        let latest_lineage = self
            .latest_lineages
            .entry(child_id.clone())
            .or_insert(lineage);
        if lineage < *latest_lineage {
            return;
        }
        if lineage > *latest_lineage {
            self.restart_policies.remove(child_id);
        }
        *latest_lineage = lineage;
        self.seen.insert(child_id.clone());

        match transition {
            // A child that is starting again has work in flight, whatever an
            // earlier generation did.
            CompletionTransition::Running => {
                self.satisfied.remove(child_id);
            }
            // The event loop aligns every installed membership from its
            // already-published snapshot, so Always-policy work is never
            // marked complete even between clean exit and restart scheduling.
            // A final snapshot realignment also checks pending restart state.
            CompletionTransition::Exited(exit) => {
                if exit.is_completed()
                    && !exit.cancelled()
                    && !self
                        .restart_policies
                        .get(child_id)
                        .is_some_and(|restart| restart.is_always())
                {
                    self.satisfied.insert(child_id.clone());
                } else {
                    self.satisfied.remove(child_id);
                }
            }
            CompletionTransition::Removed => {
                self.satisfied.insert(child_id.clone());
            }
        }
    }

    /// Rebuilds the set from `snapshot` and returns the sequence up to which
    /// lifecycle events are now redundant.
    fn realign(&mut self, snapshot: &SupervisorSnapshot) -> u64 {
        for id in &self.awaited {
            let satisfied = match snapshot.child(id) {
                Some(child) => {
                    if self
                        .latest_lineages
                        .get(id)
                        .is_some_and(|latest| *latest > child.lineage)
                    {
                        continue;
                    }
                    self.latest_lineages.insert(id.clone(), child.lineage);
                    self.restart_policies
                        .insert(id.clone(), child.restart_policy);
                    self.seen.insert(id.clone());
                    is_completed(child)
                }
                // Gone from a supervisor that once had it: removed.
                None => self.seen.contains(id),
            };
            if satisfied {
                self.satisfied.insert(id.clone());
            } else {
                self.satisfied.remove(id);
            }
        }
        snapshot.lifecycle_seq
    }
}

enum CompletionTransition<'a> {
    Running,
    Exited(&'a ExitStatus),
    Removed,
}

fn is_completed(child: &ChildSnapshot) -> bool {
    if child.membership == ChildMembershipView::Removing {
        return true;
    }
    !child.restart_policy.is_always()
        && child.next_restart_in.is_none()
        && matches!(
            &child.state,
            ChildStateView::Stopped {
                exit: Some(exit),
                ..
            }
                | ChildStateView::StartupAborted { exit }
                if exit.is_completed() && !exit.cancelled()
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::{ExitStatus, Strategy, event::ExitKind, snapshot::SupervisorStateView};

    enum TestLifecycleKind {
        Added,
        Started {
            generation: u64,
        },
        Exited {
            generation: u64,
            reason: ExitKind,
            cancelled: bool,
        },
        Removed,
    }

    fn event(seq: u64, child_id: &str, kind: TestLifecycleKind) -> LifecycleEvent {
        event_with_lineage(seq, child_id, 0, kind)
    }

    fn event_with_lineage(
        seq: u64,
        child_id: &str,
        lineage: u64,
        kind: TestLifecycleKind,
    ) -> LifecycleEvent {
        let kind = match kind {
            TestLifecycleKind::Added => LifecycleEventKind::ChildAdded {
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
            },
            TestLifecycleKind::Started { generation } => LifecycleEventKind::ChildStarted {
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
                generation,
            },
            TestLifecycleKind::Exited {
                generation,
                reason,
                cancelled,
            } => LifecycleEventKind::ChildExited {
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
                generation,
                exit: ExitStatus::new(reason, cancelled),
            },
            TestLifecycleKind::Removed => LifecycleEventKind::ChildRemoved {
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
            },
        };
        LifecycleEvent::local(kind)
    }

    fn completed(seq: u64, child_id: &str) -> LifecycleEvent {
        event(
            seq,
            child_id,
            TestLifecycleKind::Exited {
                generation: 0,
                reason: ExitKind::Completed,
                cancelled: false,
            },
        )
    }

    fn snapshot(children: Vec<ChildSnapshot>) -> SupervisorSnapshot {
        SupervisorSnapshot::new(SupervisorStateView::Running, Strategy::OneForOne, children)
    }

    #[test]
    fn an_empty_set_is_complete() {
        assert!(CompletionSet::new(Vec::<String>::new()).is_complete());
    }

    #[test]
    fn strict_initialization_validates_every_id_against_one_snapshot() {
        let mut set = CompletionSet::new(["source", "missing"]);
        let baseline = snapshot(vec![ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Running {
                previous_exit: None,
            },
        )]);

        assert_eq!(
            set.initialize(&baseline, false),
            Err(CompletionError::UnknownChild {
                child_id: "missing".to_owned(),
            })
        );
        assert!(
            set.seen.is_empty(),
            "strict validation happens before the same snapshot realigns state"
        );
    }

    #[test]
    fn future_member_initialization_realigns_from_its_validation_snapshot() {
        let mut set = CompletionSet::new(["source", "future"]);
        let mut baseline = snapshot(vec![ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Running {
                previous_exit: None,
            },
        )]);
        baseline.lifecycle_seq = 17;

        assert_eq!(set.initialize(&baseline, true), Ok(17));
        assert!(set.seen.contains("source"));
        assert!(!set.seen.contains("future"));
    }

    #[test]
    fn every_awaited_child_must_complete() {
        let mut set = CompletionSet::new(["source", "indexer"]);
        set.apply(&completed(1, "source"));
        assert!(!set.is_complete());
        set.apply(&completed(2, "indexer"));
        assert!(set.is_complete());
    }

    #[test]
    fn unawaited_children_are_ignored() {
        let mut set = CompletionSet::new(["source"]);
        set.apply(&completed(1, "metrics"));
        assert!(!set.is_complete());
    }

    #[test]
    fn a_failed_exit_does_not_complete() {
        let mut set = CompletionSet::new(["source"]);
        set.apply(&event(
            1,
            "source",
            TestLifecycleKind::Exited {
                generation: 0,
                reason: ExitKind::Failed("boom".to_owned()),
                cancelled: false,
            },
        ));
        assert!(!set.is_complete());
    }

    #[test]
    fn a_restart_un_completes_a_finished_child() {
        let mut set = CompletionSet::new(["source", "indexer"]);
        set.apply(&completed(1, "source"));
        set.apply(&event(
            2,
            "source",
            TestLifecycleKind::Started { generation: 1 },
        ));
        set.apply(&completed(3, "indexer"));
        assert!(
            !set.is_complete(),
            "a restarted child's earlier completion is stale"
        );
        set.apply(&completed(4, "source"));
        assert!(set.is_complete());
    }

    #[test]
    fn a_removed_child_leaves_the_set() {
        let mut set = CompletionSet::new(["source", "indexer"]);
        set.apply(&completed(1, "source"));
        set.apply(&event(2, "indexer", TestLifecycleKind::Removed));
        assert!(set.is_complete());
    }

    #[test]
    fn realigning_counts_a_child_that_already_completed() {
        let mut set = CompletionSet::new(["source"]);
        let source = ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Stopped {
                started: true,
                exit: Some(ExitStatus::new(ExitKind::Completed, false)),
            },
        );
        let seq = set.realign(&snapshot(vec![source]));
        assert_eq!(seq, 0);
        assert!(set.is_complete());
    }

    #[test]
    fn realigning_does_not_count_a_pending_restart() {
        let mut set = CompletionSet::new(["source"]);
        let mut source = ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Stopped {
                started: true,
                exit: Some(ExitStatus::new(ExitKind::Completed, false)),
            },
        );
        source.next_restart_in = Some(std::time::Duration::from_millis(10));
        set.realign(&snapshot(vec![source]));
        assert!(!set.is_complete());
    }

    #[test]
    fn realigning_does_not_count_a_cancelled_completed_exit() {
        let mut set = CompletionSet::new(["source"]);
        let source = ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Stopped {
                started: true,
                exit: Some(ExitStatus::new(ExitKind::Completed, true)),
            },
        );
        set.realign(&snapshot(vec![source]));
        assert!(!set.is_complete());
    }

    #[test]
    fn realigning_counts_a_clean_startup_abort() {
        let mut set = CompletionSet::new(["source"]);
        let source = ChildSnapshot::new(
            "source",
            0,
            ChildStateView::StartupAborted {
                exit: ExitStatus::new(ExitKind::Completed, false),
            },
        );

        set.realign(&snapshot(vec![source]));

        assert!(set.is_complete());
    }

    #[test]
    fn realigning_drops_a_completion_the_child_has_outlived() {
        let mut set = CompletionSet::new(["source"]);
        set.apply(&completed(1, "source"));
        set.realign(&snapshot(vec![ChildSnapshot::new(
            "source",
            1,
            ChildStateView::Running {
                previous_exit: None,
            },
        )]));
        assert!(!set.is_complete(), "the child is running again");
    }

    #[test]
    fn displaced_membership_events_do_not_change_replacement_completion() {
        let mut set = CompletionSet::new(["source"]);
        let mut source = ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Running {
                previous_exit: None,
            },
        );
        source.lineage = 2;
        set.realign(&snapshot(vec![source]));

        set.apply(&event_with_lineage(
            1,
            "source",
            1,
            TestLifecycleKind::Removed,
        ));
        assert!(
            !set.is_complete(),
            "an old removal must not complete the replacement membership"
        );

        set.apply(&event_with_lineage(
            2,
            "source",
            2,
            TestLifecycleKind::Exited {
                generation: 0,
                reason: ExitKind::Completed,
                cancelled: false,
            },
        ));
        assert!(set.is_complete());

        set.apply(&event_with_lineage(
            3,
            "source",
            1,
            TestLifecycleKind::Exited {
                generation: 0,
                reason: ExitKind::Completed,
                cancelled: true,
            },
        ));
        assert!(
            set.is_complete(),
            "an old cancelled exit must not un-complete the replacement membership"
        );
    }

    #[test]
    fn realigning_treats_a_previously_seen_absence_as_removal() {
        let mut set = CompletionSet::new(["source"]);
        set.apply(&event(1, "source", TestLifecycleKind::Added));
        set.realign(&snapshot(Vec::new()));
        assert!(set.is_complete());
    }

    #[test]
    fn realigning_does_not_complete_a_child_that_never_existed() {
        let mut set = CompletionSet::new(["typo"]);
        set.realign(&snapshot(vec![ChildSnapshot::new(
            "source",
            0,
            ChildStateView::Running {
                previous_exit: None,
            },
        )]));
        assert!(
            !set.is_complete(),
            "an id that was never a member must not be mistaken for a removal"
        );
    }
}
