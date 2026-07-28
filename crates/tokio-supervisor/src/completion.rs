//! Tracking children that represent finite work.
//!
//! A supervisor normally runs until it is told to stop. Pipeline and batch
//! subtrees invert that: some children have a natural completion point, and the
//! scope's job is done once they reach it. This module expresses that as a
//! reduction over [`watch_lifecycle`](crate::SupervisorHandle::watch_lifecycle)
//! rather than as supervisor configuration, so the completion rule lives with
//! the code that cares about it instead of in the control loop.

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use tokio_util::sync::CancellationToken;

use crate::{
    ExitStatusView,
    handle::SupervisorHandle,
    lifecycle::LifecycleEvent,
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

/// Cancellation guard for a
/// [`shutdown_on_completion`](crate::SupervisorHandle::shutdown_on_completion)
/// task.
///
/// Dropping the guard cancels the watch, leaving the supervisor running. The
/// task also stops on its own once it has requested shutdown, or once the
/// watched supervisor identity becomes terminal.
#[must_use = "dropping the guard immediately cancels the completion watch"]
pub struct CompletionGuard {
    cancellation: CancellationToken,
}

impl CompletionGuard {
    /// Cancels the completion watch.
    ///
    /// Cancellation is idempotent. A shutdown already requested cannot be
    /// retracted.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

impl fmt::Debug for CompletionGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionGuard")
            .field("is_cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl SupervisorHandle {
    /// Waits until every child in `ids` is simultaneously in a completed
    /// state.
    ///
    /// A child counts as completed once its current generation has exited with
    /// [`ExitStatusView::Completed`] and no restart is pending for it. Any
    /// later start un-completes it, so a child that is restarted — including by
    /// a sibling-driven group restart — must complete again. Failed exits never
    /// count, matching the rule that failures follow the restart policy rather
    /// than signalling finished work. A child whose membership is removed drops
    /// out of the set: its work is not coming back.
    ///
    /// Awaiting an empty set returns [`CompletionOutcome::Completed`]
    /// immediately. Awaiting an id that never becomes a child of this
    /// supervisor never completes.
    ///
    /// The wait is gap-free from the moment it is called: it aligns a
    /// lifecycle watch against a snapshot, so children that completed earlier
    /// are still counted, and it realigns from a fresh snapshot if the watch
    /// reports [`LifecycleEvent::Lagged`]. Calling it on a pre-spawn
    /// handle is well defined — statically configured children are projected
    /// before the scope starts.
    ///
    /// ```no_run
    /// use tokio_supervisor::{CompletionOutcome, SupervisorHandle};
    ///
    /// # async fn example(handle: SupervisorHandle) {
    /// if handle.wait_completed(["source", "indexer"]).await == CompletionOutcome::Completed {
    ///     handle.shutdown();
    /// }
    /// # }
    /// ```
    pub async fn wait_completed<I, S>(&self, ids: I) -> CompletionOutcome
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        wait_completed(self, CompletionSet::new(ids)).await
    }

    /// Shuts this supervisor down once every child in `ids` has completed.
    ///
    /// This is the fire-and-forget form of
    /// [`wait_completed`](Self::wait_completed), and the usual way to express
    /// a subtree whose lifetime is bounded by finite work. Set it up before
    /// spawning, from a pre-spawn handle, so a child that finishes immediately
    /// is still observed:
    ///
    /// ```no_run
    /// use tokio_supervisor::{ChildSpec, RestartPolicy, Supervisor};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let builder = Supervisor::ordered()
    ///     .child(ChildSpec::task("source", |_| async { Ok(()) }).restart(RestartPolicy::OnFailure))
    ///     .child(ChildSpec::task("indexer", |_| async { Ok(()) }).restart(RestartPolicy::Never));
    /// let handle = builder.handle();
    /// let _finished = handle.shutdown_on_completion(["source", "indexer"]);
    /// let supervisor = builder.build()?;
    /// # let _ = supervisor;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The returned guard must be retained: dropping it cancels the watch and
    /// leaves the supervisor running. The spawned task holds no lifecycle
    /// lease, so it never keeps a root supervisor alive on its own.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn shutdown_on_completion<I, S>(&self, ids: I) -> CompletionGuard
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set = CompletionSet::new(ids);
        let handle = self.observer();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();

        tokio::spawn(async move {
            let _cancel_on_exit = task_cancellation.clone().drop_guard();
            let outcome = tokio::select! {
                biased;
                () = task_cancellation.cancelled() => return,
                outcome = wait_completed(&handle, set) => outcome,
            };
            if outcome == CompletionOutcome::Completed {
                handle.shutdown();
            }
        });

        CompletionGuard { cancellation }
    }
}

async fn wait_completed(handle: &SupervisorHandle, mut set: CompletionSet) -> CompletionOutcome {
    // The watch is created before the snapshot is read so no transition can
    // fall between them; events the snapshot already reflects are then
    // discarded by sequence.
    let mut watch = handle.watch_lifecycle();
    let mut baseline = set.realign(&handle.snapshot());

    loop {
        if set.is_complete() {
            return CompletionOutcome::Completed;
        }
        let Some(event) = watch.next().await else {
            return CompletionOutcome::Closed;
        };
        if matches!(event, LifecycleEvent::Lagged { .. }) {
            // A dropped prefix may have contained transitions for awaited
            // children, so edge-derived state has to be rebuilt from state.
            baseline = set.realign(&handle.snapshot());
        } else if direct_child_seq(&event).is_some_and(|seq| seq > baseline) {
            set.apply(&event);
        }
    }
}

/// The reduction behind [`SupervisorHandle::wait_completed`].
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
        }
    }

    fn is_complete(&self) -> bool {
        self.awaited.iter().all(|id| self.satisfied.contains(id))
    }

    fn awaits(&self, id: &str) -> bool {
        self.awaited.iter().any(|awaited| awaited == id)
    }

    fn apply(&mut self, event: &LifecycleEvent) {
        let (child_id, lineage, transition) = match event {
            LifecycleEvent::Added {
                child_id, lineage, ..
            } => (child_id, *lineage, CompletionTransition::Running),
            LifecycleEvent::Started {
                child_id, lineage, ..
            } => (child_id, *lineage, CompletionTransition::Running),
            LifecycleEvent::Exited {
                child_id,
                lineage,
                reason,
                cancelled,
                ..
            } => (
                child_id,
                *lineage,
                CompletionTransition::Exited {
                    reason,
                    cancelled: *cancelled,
                },
            ),
            LifecycleEvent::Removed {
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
        *latest_lineage = lineage;
        self.seen.insert(child_id.clone());

        match transition {
            // A child that is starting again has work in flight, whatever an
            // earlier generation did.
            CompletionTransition::Running => {
                self.satisfied.remove(child_id);
            }
            // A cancellation-driven `Ok(())` — shutdown, removal, or a
            // sibling-driven group restart — is not finished work.
            CompletionTransition::Exited {
                reason, cancelled, ..
            } => {
                if matches!(reason, ExitStatusView::Completed) && !cancelled {
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
    Exited {
        reason: &'a ExitStatusView,
        cancelled: bool,
    },
    Removed,
}

fn direct_child_seq(event: &LifecycleEvent) -> Option<u64> {
    match event {
        LifecycleEvent::Added {
            supervisor_path,
            seq,
            ..
        }
        | LifecycleEvent::Started {
            supervisor_path,
            seq,
            ..
        }
        | LifecycleEvent::Exited {
            supervisor_path,
            seq,
            ..
        }
        | LifecycleEvent::Removed {
            supervisor_path,
            seq,
            ..
        } if supervisor_path.is_empty() => Some(*seq),
        _ => None,
    }
}

fn is_completed(child: &ChildSnapshot) -> bool {
    if child.membership == ChildMembershipView::Removing {
        return true;
    }
    child.state == ChildStateView::Stopped
        && child.next_restart_in.is_none()
        && matches!(child.last_exit, Some(ExitStatusView::Completed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Strategy, snapshot::SupervisorStateView};

    enum TestLifecycleKind {
        Added,
        Started {
            generation: u64,
        },
        Exited {
            generation: u64,
            reason: ExitStatusView,
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
        match kind {
            TestLifecycleKind::Added => LifecycleEvent::Added {
                supervisor_path: Vec::new(),
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
            },
            TestLifecycleKind::Started { generation } => LifecycleEvent::Started {
                supervisor_path: Vec::new(),
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
            } => LifecycleEvent::Exited {
                supervisor_path: Vec::new(),
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
                generation,
                reason,
                cancelled,
            },
            TestLifecycleKind::Removed => LifecycleEvent::Removed {
                supervisor_path: Vec::new(),
                seq,
                child_id: child_id.to_owned(),
                lineage,
                total_restarts: 0,
                child_restart_count: 0,
            },
        }
    }

    fn completed(seq: u64, child_id: &str) -> LifecycleEvent {
        event(
            seq,
            child_id,
            TestLifecycleKind::Exited {
                generation: 0,
                reason: ExitStatusView::Completed,
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
                reason: ExitStatusView::Failed("boom".to_owned()),
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
        let mut source = ChildSnapshot::new("source", 0, ChildStateView::Stopped);
        source.last_exit = Some(ExitStatusView::Completed);
        let seq = set.realign(&snapshot(vec![source]));
        assert_eq!(seq, 0);
        assert!(set.is_complete());
    }

    #[test]
    fn realigning_does_not_count_a_pending_restart() {
        let mut set = CompletionSet::new(["source"]);
        let mut source = ChildSnapshot::new("source", 0, ChildStateView::Stopped);
        source.last_exit = Some(ExitStatusView::Completed);
        source.next_restart_in = Some(std::time::Duration::from_millis(10));
        set.realign(&snapshot(vec![source]));
        assert!(!set.is_complete());
    }

    #[test]
    fn realigning_drops_a_completion_the_child_has_outlived() {
        let mut set = CompletionSet::new(["source"]);
        set.apply(&completed(1, "source"));
        set.realign(&snapshot(vec![ChildSnapshot::new(
            "source",
            1,
            ChildStateView::Running,
        )]));
        assert!(!set.is_complete(), "the child is running again");
    }

    #[test]
    fn displaced_membership_events_do_not_change_replacement_completion() {
        let mut set = CompletionSet::new(["source"]);
        let mut source = ChildSnapshot::new("source", 0, ChildStateView::Running);
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
                reason: ExitStatusView::Completed,
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
                reason: ExitStatusView::Completed,
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
            ChildStateView::Running,
        )]));
        assert!(
            !set.is_complete(),
            "an id that was never a member must not be mistaken for a removal"
        );
    }
}
