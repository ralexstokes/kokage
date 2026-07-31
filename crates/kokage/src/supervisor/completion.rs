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
    CancellationToken, ChildExitView, CompletionOnDrop, Guard, LifecycleEvent, LifecycleEventKind,
    ScopeKind,
    handle::SupervisorHandle,
    snapshot::{ChildMembershipView, ChildSnapshot, ChildStateView, SupervisorSnapshot},
};

/// How a completion wait ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a completion wait reports whether the work finished or the supervisor stopped"]
#[non_exhaustive]
pub enum CompletionOutcome {
    /// Every awaited child was simultaneously in a completed state.
    Completed,
    /// The watched supervisor identity became terminal before the awaited
    /// children completed, so the condition can never be satisfied.
    Closed,
}

/// Error returned when a completion watch cannot use its requested mode or ids.
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
}

/// A configurable watch for direct children of one scope to complete.
///
/// Create a watch with [`ScopeRef::completions`](crate::ScopeRef::completions). It is
/// strict by default: every id must name current or projected pre-spawn
/// membership. [`allow_future_members`](Self::allow_future_members) opts a
/// dynamic scope into waiting for later insertion, and
/// [`then_shutdown`](Self::then_shutdown) arms shutdown without awaiting the
/// result directly.
#[must_use = "a completion watch must be awaited or armed"]
pub struct CompletionWatch {
    handle: SupervisorHandle,
    kind: ScopeKind,
    set: CompletionSet,
    allow_future_members: bool,
    error: Option<CompletionError>,
}

impl CompletionWatch {
    pub(crate) fn new<I, S>(handle: SupervisorHandle, kind: ScopeKind, ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            handle,
            kind,
            set: CompletionSet::new(ids),
            allow_future_members: false,
            error: None,
        }
    }

    /// Treats absent ids as membership that may be inserted later.
    ///
    /// This mode is valid only for a dynamic scope. On an ordered scope,
    /// awaiting the watch returns [`CompletionError::NotDynamic`]. An armed
    /// shutdown watch logs the error and completes without requesting shutdown.
    pub fn allow_future_members(mut self) -> Self {
        if self.kind == ScopeKind::Dynamic {
            self.allow_future_members = true;
        } else {
            self.error = Some(CompletionError::NotDynamic);
        }
        self
    }

    async fn outcome(self) -> Result<CompletionOutcome, CompletionError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        reduce_completion(&self.handle, self.set, self.allow_future_members).await
    }

    /// Shuts this scope down once every named child has completed.
    ///
    /// This is the fire-and-forget form of
    /// [`wait`](CompletionWatch::wait), and the usual way to express
    /// a subtree whose lifetime is bounded by finite work. Set it up before
    /// spawning, from a pre-spawn handle, so a child that finishes immediately
    /// is still observed.
    ///
    /// The returned guard cancels the watch when dropped and leaves the
    /// supervisor running; retain it for as long as revoking the shutdown
    /// should stay possible, or consume it with [`Guard::detach`] for true
    /// fire-and-forget. The spawned task holds no lifecycle
    /// ownership, so it never keeps a root supervisor alive on its own.
    /// Errors produced while evaluating the completion condition are logged
    /// with the requested child ids and then discarded; they do not request
    /// shutdown.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn then_shutdown(self) -> Guard {
        let child_ids = self.set.awaited.clone();
        let scope_kind = self.kind;
        let handle = self.handle.clone();
        let cancellation = CancellationToken::new();
        let (finished, finished_on_drop) = CompletionOnDrop::armed();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let _finished_on_drop = finished_on_drop;
            let outcome = tokio::select! {
                biased;
                () = task_cancellation.cancelled() => return,
                outcome = self.outcome() => outcome,
            };
            match outcome {
                Ok(CompletionOutcome::Completed) => handle.shutdown(),
                Ok(CompletionOutcome::Closed) => {}
                Err(error) => {
                    tracing::warn!(
                        %error,
                        ?scope_kind,
                        ?child_ids,
                        "completion-triggered shutdown watch failed"
                    );
                }
            }
        });

        std::mem::drop(task);
        Guard::from_tokens(cancellation, finished)
    }

    /// Waits until every named child is simultaneously completed.
    ///
    /// A child counts as completed once its current generation has exited with
    /// [`ChildExitView::Completed`] and no restart is pending for it. Any
    /// later start un-completes it, so a child that is restarted — including by
    /// a sibling-driven group restart — must complete again. Failed exits never
    /// count, matching the rule that failures follow the restart policy rather
    /// than signalling finished work. A child configured with
    /// [`Restart::always()`](crate::Restart::always()) never counts as
    /// completed while it remains a member, even between its clean exit and
    /// replacement. A child whose membership is removed drops out of the set:
    /// its work is not coming back.
    ///
    /// Awaiting an empty set returns [`CompletionOutcome::Completed`]
    /// immediately. In the default strict mode, an id absent from the current
    /// (including projected pre-spawn) membership returns
    /// [`CompletionError::UnknownChild`].
    ///
    /// The wait is gap-free from the moment it is called: it installs a
    /// lifecycle watch before taking the single snapshot used for both strict
    /// validation and state alignment. Children that completed earlier are
    /// still counted, and the reducer realigns from a fresh snapshot if the
    /// watch reports [`LifecycleEventKind::Lagged`]. Calling it on a pre-spawn
    /// handle is well defined — statically configured children are projected
    /// before the scope starts.
    ///
    /// Awaiting a set that contains the current actor from one of that actor's
    /// callbacks can deadlock when completion depends on the callback returning.
    /// Use [`Context::offload`](crate::Context::offload) when the result must
    /// return to an actor as a later message.
    pub async fn wait(self) -> Result<CompletionOutcome, CompletionError> {
        self.outcome().await
    }
}

impl SupervisorHandle {
    pub(crate) fn completions<I, S>(&self, ids: I) -> CompletionWatch
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CompletionWatch::new(self.clone(), self.kind(), ids)
    }
}

async fn reduce_completion(
    handle: &SupervisorHandle,
    mut set: CompletionSet,
    allow_future_members: bool,
) -> Result<CompletionOutcome, CompletionError> {
    // The watch is created before the snapshot is read so no transition can
    // fall between them; events the snapshot already reflects are then
    // discarded by sequence.
    let mut watch = handle.watch_lifecycle().direct_children();
    let snapshot = handle.snapshot();
    let mut baseline = set.initialize(&snapshot, allow_future_members)?;

    loop {
        if set.is_complete() {
            // `Exited` is emitted before its immediately following
            // restart-scheduled transition. Recheck state before completing so
            // `Restart::always()` cannot expose that transient stop as
            // finished work.
            baseline = set.realign(&handle.snapshot());
            if set.is_complete() {
                return Ok(CompletionOutcome::Completed);
            }
        }
        let Some(event) = watch.next().await else {
            return Ok(CompletionOutcome::Closed);
        };
        if matches!(event.kind, LifecycleEventKind::Lagged { .. }) {
            // A dropped prefix may have contained transitions for awaited
            // children, so edge-derived state has to be rebuilt from state.
            baseline = set.realign(&handle.snapshot());
        } else if event.scope_path.is_empty() && event.seq().is_some_and(|seq| seq > baseline) {
            let needs_realign = matches!(
                &event.kind,
                LifecycleEventKind::ChildAdded { .. } | LifecycleEventKind::ChildExited { .. }
            );
            set.apply(&event);
            if needs_realign {
                // The snapshot is published before this exit event and knows
                // the restart policy and whether a replacement is pending.
                // Realigning here avoids treating an Always-policy clean exit
                // as complete even transiently.
                baseline = set.realign(&handle.snapshot());
            }
        }
    }
}

/// The reduction behind [`CompletionWatch::wait`].
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
    restart_policies: HashMap<String, crate::supervisor::Restart>,
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
    Exited(&'a ChildExitView),
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
    use crate::supervisor::{
        ChildExitView, Strategy, event::ExitKind, snapshot::SupervisorStateView,
    };

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
                exit: ChildExitView::new(reason, cancelled),
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
                exit: Some(ChildExitView::new(ExitKind::Completed, false)),
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
                exit: Some(ChildExitView::new(ExitKind::Completed, false)),
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
                exit: Some(ChildExitView::new(ExitKind::Completed, true)),
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
                exit: ChildExitView::new(ExitKind::Completed, false),
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
