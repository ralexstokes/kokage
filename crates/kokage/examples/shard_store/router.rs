use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorRef, ActorSpec, CallError, Context, DynamicScopeRef, ExitResult, Reply,
    RestartPolicy, ScopeRef, Tree,
};
use tokio::{sync::Notify, time::Instant};

use crate::{
    directory::{DirectoryMsg, Endpoint},
    model::{
        DirectorySnapshot, DurableImage, Key, KeyRange, PlannedChange, ReadReceipt,
        RestartEvidence, RouteView, ShardConfig, TransitionReport, Write, WriteReceipt,
    },
    shard::{DurableShard, Shard, ShardMsg},
};

const CALL_BOUND: Duration = Duration::from_secs(2);
const HANDOFF_BOUND: Duration = Duration::from_secs(4);
const PHASE_BOUND: Duration = Duration::from_secs(3);
const TRANSITION_BOUND: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailurePoint {
    FirstMount,
    SecondMount,
    BeforeCutover,
    CutoverReplyLost,
    BeforeRetire,
    RetireReplyLost,
}

#[derive(Clone, Default)]
pub(crate) struct FaultInjector {
    armed: Arc<Mutex<VecDeque<FailurePoint>>>,
}

impl FaultInjector {
    #[cfg(test)]
    pub fn arm(&self, point: FailurePoint) {
        self.armed
            .lock()
            .expect("fault injector lock is not poisoned")
            .push_back(point);
    }

    fn take(&self, point: FailurePoint) -> bool {
        let mut armed = self
            .armed
            .lock()
            .expect("fault injector lock is not poisoned");
        if armed.front() == Some(&point) {
            armed.pop_front();
            true
        } else {
            false
        }
    }

    fn fail(&self, point: FailurePoint) -> Result<(), String> {
        if self.take(point) {
            Err(format!("injected transition failure at {point:?}"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Default)]
pub struct TransitionGate {
    held: Arc<AtomicBool>,
    entries: Arc<AtomicU64>,
    entered: Arc<Notify>,
    released: Arc<Notify>,
    buffered: Arc<AtomicU64>,
    buffered_changed: Arc<Notify>,
    recovery_held: Arc<AtomicBool>,
    recovery_entries: Arc<AtomicU64>,
    recovery_entered: Arc<Notify>,
    recovery_released: Arc<Notify>,
}

impl TransitionGate {
    pub fn arm(&self) -> u64 {
        self.held.store(true, Ordering::Release);
        self.entries.load(Ordering::Acquire) + 1
    }

    pub async fn wait_entered(&self, ticket: u64) {
        while self.entries.load(Ordering::Acquire) < ticket {
            self.entered.notified().await;
        }
    }

    pub fn release(&self) {
        self.held.store(false, Ordering::Release);
        self.released.notify_waiters();
    }

    pub fn buffered(&self) -> u64 {
        self.buffered.load(Ordering::Acquire)
    }

    pub async fn wait_buffered(&self, target: u64) {
        while self.buffered.load(Ordering::Acquire) < target {
            self.buffered_changed.notified().await;
        }
    }

    #[cfg(test)]
    pub fn arm_recovery(&self) -> u64 {
        self.recovery_held.store(true, Ordering::Release);
        self.recovery_entries.load(Ordering::Acquire) + 1
    }

    #[cfg(test)]
    pub async fn wait_recovery_entered(&self, ticket: u64) {
        while self.recovery_entries.load(Ordering::Acquire) < ticket {
            self.recovery_entered.notified().await;
        }
    }

    #[cfg(test)]
    pub fn release_recovery(&self) {
        self.recovery_held.store(false, Ordering::Release);
        self.recovery_released.notify_waiters();
    }

    fn record_buffered(&self) {
        self.buffered.fetch_add(1, Ordering::AcqRel);
        self.buffered_changed.notify_waiters();
    }

    async fn pause_if_armed(&self) {
        if !self.held.load(Ordering::Acquire) {
            return;
        }
        self.entries.fetch_add(1, Ordering::AcqRel);
        self.entered.notify_waiters();
        while self.held.load(Ordering::Acquire) {
            self.released.notified().await;
        }
    }

    async fn pause_recovery_if_armed(&self) {
        if !self.recovery_held.load(Ordering::Acquire) {
            return;
        }
        self.recovery_entries.fetch_add(1, Ordering::AcqRel);
        self.recovery_entered.notify_waiters();
        while self.recovery_held.load(Ordering::Acquire) {
            self.recovery_released.notified().await;
        }
    }
}

#[derive(Clone)]
struct Member {
    endpoint: Endpoint,
    scope: ScopeRef,
    durable: Arc<DurableShard>,
}

#[derive(Clone)]
struct TransitionContext {
    shards: DynamicScopeRef,
    directory: ActorRef<DirectoryMsg>,
    gate: TransitionGate,
    faults: FaultInjector,
}

enum BufferedRequest {
    Write {
        command: Write,
        reply: Reply<Result<WriteReceipt, String>>,
    },
    Read {
        key: Key,
        reply: Reply<Result<ReadReceipt, String>>,
    },
}

impl BufferedRequest {
    fn key(&self) -> Key {
        match self {
            Self::Write { command, .. } => command.key,
            Self::Read { key, .. } => *key,
        }
    }
}

enum PendingReply {
    Bootstrap(Reply<Result<DirectorySnapshot, String>>),
    Change(Reply<Result<TransitionReport, String>>),
}

struct Pending {
    ranges: Vec<KeyRange>,
    buffered: Vec<BufferedRequest>,
    reply: PendingReply,
}

pub(crate) struct TransitionOutcome {
    removed: Vec<String>,
    members: Vec<Member>,
    directory: DirectorySnapshot,
    report: Option<TransitionReport>,
}

pub(crate) enum RouterMsg {
    Bootstrap {
        range: KeyRange,
        config: ShardConfig,
        reply: Reply<Result<DirectorySnapshot, String>>,
    },
    Write {
        command: Write,
        reply: Reply<Result<WriteReceipt, String>>,
    },
    Read {
        key: Key,
        reply: Reply<Result<ReadReceipt, String>>,
    },
    Split {
        source_id: String,
        at: Key,
        reply: Reply<Result<TransitionReport, String>>,
    },
    Reload {
        source_id: String,
        config: ShardConfig,
        crash_during_handoff: bool,
        reply: Reply<Result<TransitionReport, String>>,
    },
    RequestFinishedWrite {
        reply: Reply<Result<WriteReceipt, String>>,
        result: Result<WriteReceipt, String>,
    },
    RequestFinishedRead {
        reply: Reply<Result<ReadReceipt, String>>,
        result: Result<ReadReceipt, String>,
    },
    TransitionFinished(Result<TransitionOutcome, String>),
}

pub(crate) struct MembershipRouter {
    shards: DynamicScopeRef,
    directory: ActorRef<DirectoryMsg>,
    gate: TransitionGate,
    faults: FaultInjector,
    members: Vec<Member>,
    pending: Option<Pending>,
    next_epoch: u64,
}

impl MembershipRouter {
    pub(crate) fn with_faults(
        shards: DynamicScopeRef,
        directory: ActorRef<DirectoryMsg>,
        gate: TransitionGate,
        faults: FaultInjector,
    ) -> Self {
        Self {
            shards,
            directory,
            gate,
            faults,
            members: Vec::new(),
            pending: None,
            next_epoch: 0,
        }
    }

    fn allocate_epoch(&mut self) -> u64 {
        self.next_epoch += 1;
        self.next_epoch
    }

    fn member(&self, id: &str) -> Option<Member> {
        self.members
            .iter()
            .find(|member| member.endpoint.view.shard_id == id)
            .cloned()
    }

    fn is_buffered(&self, key: Key) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.ranges.iter().any(|range| range.contains(key)))
    }

    fn dispatch(&self, request: BufferedRequest, ctx: &mut Context<'_, Self>) {
        match request {
            BufferedRequest::Write { command, reply } => {
                let directory = self.directory.clone();
                ctx.offload(
                    CALL_BOUND,
                    execute_write(directory, command),
                    move |result| RouterMsg::RequestFinishedWrite {
                        reply,
                        result: flatten_offload(result),
                    },
                );
            }
            BufferedRequest::Read { key, reply } => {
                let directory = self.directory.clone();
                ctx.offload(CALL_BOUND, execute_read(directory, key), move |result| {
                    RouterMsg::RequestFinishedRead {
                        reply,
                        result: flatten_offload(result),
                    }
                });
            }
        }
    }

    fn start_bootstrap(
        &mut self,
        range: KeyRange,
        config: ShardConfig,
        reply: Reply<Result<DirectorySnapshot, String>>,
        ctx: &mut Context<'_, Self>,
    ) {
        if self.pending.is_some() || !self.members.is_empty() {
            reply.send(Err("store is already bootstrapped or changing".to_owned()));
            return;
        }
        let epoch = self.allocate_epoch();
        let shards = self.shards.clone();
        let directory = self.directory.clone();
        self.pending = Some(Pending {
            ranges: vec![range],
            buffered: Vec::new(),
            reply: PendingReply::Bootstrap(reply),
        });
        ctx.offload(
            TRANSITION_BOUND,
            async move { bootstrap(shards, directory, range, config, epoch).await },
            |result| RouterMsg::TransitionFinished(flatten_offload(result)),
        );
    }

    fn start_split(
        &mut self,
        source_id: String,
        at: Key,
        reply: Reply<Result<TransitionReport, String>>,
        ctx: &mut Context<'_, Self>,
    ) {
        if self.pending.is_some() {
            reply.send(Err("another membership change is in progress".to_owned()));
            return;
        }
        let Some(source) = self.member(&source_id) else {
            reply.send(Err(format!("unknown source shard {source_id}")));
            return;
        };
        if !(source.endpoint.view.range.start < at && at < source.endpoint.view.range.end) {
            reply.send(Err(format!("split point {at} is outside {source_id}")));
            return;
        }
        let epochs = (self.allocate_epoch(), self.allocate_epoch());
        let transition = TransitionContext {
            shards: self.shards.clone(),
            directory: self.directory.clone(),
            gate: self.gate.clone(),
            faults: self.faults.clone(),
        };
        self.pending = Some(Pending {
            ranges: vec![source.endpoint.view.range],
            buffered: Vec::new(),
            reply: PendingReply::Change(reply),
        });
        ctx.offload(
            TRANSITION_BOUND,
            async move {
                transition.gate.pause_if_armed().await;
                split(transition, source, at, epochs).await
            },
            |result| RouterMsg::TransitionFinished(flatten_offload(result)),
        );
    }

    fn start_reload(
        &mut self,
        source_id: String,
        config: ShardConfig,
        crash_during_handoff: bool,
        reply: Reply<Result<TransitionReport, String>>,
        ctx: &mut Context<'_, Self>,
    ) {
        if self.pending.is_some() {
            reply.send(Err("another membership change is in progress".to_owned()));
            return;
        }
        let Some(source) = self.member(&source_id) else {
            reply.send(Err(format!("unknown source shard {source_id}")));
            return;
        };
        let epoch = self.allocate_epoch();
        let transition = TransitionContext {
            shards: self.shards.clone(),
            directory: self.directory.clone(),
            gate: self.gate.clone(),
            faults: self.faults.clone(),
        };
        self.pending = Some(Pending {
            ranges: vec![source.endpoint.view.range],
            buffered: Vec::new(),
            reply: PendingReply::Change(reply),
        });
        ctx.offload(
            TRANSITION_BOUND,
            async move {
                transition.gate.pause_if_armed().await;
                reload(transition, source, config, epoch, crash_during_handoff).await
            },
            |result| RouterMsg::TransitionFinished(flatten_offload(result)),
        );
    }

    fn finish_transition(
        &mut self,
        outcome: Result<TransitionOutcome, String>,
        ctx: &mut Context<'_, Self>,
    ) {
        let Some(mut pending) = self.pending.take() else {
            panic!("transition completion without a pending transition");
        };
        let buffered_count = pending.buffered.len();

        match outcome {
            Ok(mut outcome) => {
                self.members
                    .retain(|member| !outcome.removed.contains(&member.endpoint.view.shard_id));
                self.members.extend(outcome.members);
                self.members
                    .sort_by_key(|member| member.endpoint.view.range);

                for request in pending.buffered.drain(..) {
                    self.dispatch(request, ctx);
                }

                match pending.reply {
                    PendingReply::Bootstrap(reply) => reply.send(Ok(outcome.directory)),
                    PendingReply::Change(reply) => {
                        let mut report = outcome
                            .report
                            .take()
                            .expect("planned change returns a transition report");
                        report.buffered_requests = buffered_count;
                        reply.send(Ok(report));
                    }
                }
            }
            Err(error) => {
                for request in pending.buffered.drain(..) {
                    self.dispatch(request, ctx);
                }
                match pending.reply {
                    PendingReply::Bootstrap(reply) => reply.send(Err(error)),
                    PendingReply::Change(reply) => reply.send(Err(error)),
                }
            }
        }
    }
}

impl Actor for MembershipRouter {
    type Msg = RouterMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            RouterMsg::Bootstrap {
                range,
                config,
                reply,
            } => self.start_bootstrap(range, config, reply, ctx),
            RouterMsg::Write { command, reply } => {
                let request = BufferedRequest::Write { command, reply };
                if self.is_buffered(request.key()) {
                    self.gate.record_buffered();
                    self.pending
                        .as_mut()
                        .expect("buffer predicate requires a transition")
                        .buffered
                        .push(request);
                } else {
                    self.dispatch(request, ctx);
                }
            }
            RouterMsg::Read { key, reply } => {
                let request = BufferedRequest::Read { key, reply };
                if self.is_buffered(request.key()) {
                    self.gate.record_buffered();
                    self.pending
                        .as_mut()
                        .expect("buffer predicate requires a transition")
                        .buffered
                        .push(request);
                } else {
                    self.dispatch(request, ctx);
                }
            }
            RouterMsg::Split {
                source_id,
                at,
                reply,
            } => self.start_split(source_id, at, reply, ctx),
            RouterMsg::Reload {
                source_id,
                config,
                crash_during_handoff,
                reply,
            } => self.start_reload(source_id, config, crash_during_handoff, reply, ctx),
            RouterMsg::RequestFinishedWrite { reply, result } => reply.send(result),
            RouterMsg::RequestFinishedRead { reply, result } => reply.send(result),
            RouterMsg::TransitionFinished(outcome) => self.finish_transition(outcome, ctx),
        }
        Ok(())
    }
}

fn flatten_offload<T>(
    result: Result<Result<T, String>, kokage::OffloadDeadline>,
) -> Result<T, String> {
    result.map_err(|_| "router offload deadline elapsed".to_owned())?
}

async fn resolve(directory: &ActorRef<DirectoryMsg>, key: Key) -> Result<Endpoint, String> {
    directory
        .call(|reply| DirectoryMsg::Resolve { key, reply }, CALL_BOUND)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("no shard owns key {key}"))
}

async fn execute_write(
    directory: ActorRef<DirectoryMsg>,
    command: Write,
) -> Result<WriteReceipt, String> {
    let endpoint = resolve(&directory, command.key).await?;
    endpoint
        .shard
        .call(|reply| ShardMsg::Write { command, reply }, CALL_BOUND)
        .await
        .map_err(|error| error.to_string())?
}

async fn execute_read(directory: ActorRef<DirectoryMsg>, key: Key) -> Result<ReadReceipt, String> {
    let endpoint = resolve(&directory, key).await?;
    endpoint
        .shard
        .call(|reply| ShardMsg::Read { key, reply }, CALL_BOUND)
        .await
        .map_err(|error| error.to_string())?
}

async fn bootstrap(
    shards: DynamicScopeRef,
    directory: ActorRef<DirectoryMsg>,
    range: KeyRange,
    config: ShardConfig,
    epoch: u64,
) -> Result<TransitionOutcome, String> {
    let member = mount(
        &shards,
        range,
        epoch,
        "blue",
        DurableImage::empty(range, config),
    )
    .await?;
    let (snapshot, _) = cutover(
        &directory,
        format!("bootstrap-e{epoch}"),
        Vec::new(),
        vec![member.endpoint.clone()],
        false,
    )
    .await?;
    Ok(TransitionOutcome {
        removed: Vec::new(),
        members: vec![member],
        directory: snapshot,
        report: None,
    })
}

async fn split(
    transition: TransitionContext,
    source: Member,
    at: Key,
    epochs: (u64, u64),
) -> Result<TransitionOutcome, String> {
    let TransitionContext {
        shards,
        directory,
        gate,
        faults,
    } = transition;
    let source_id = source.endpoint.view.shard_id.clone();
    let operation_id = format!("split-{source_id}-at-{at}-e{}-e{}", epochs.0, epochs.1);
    let (image, restart, recovered_crash) =
        match prepare(&source, operation_id.clone(), false, &gate).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = source.durable.abort_handoff(&operation_id);
                return Err(error);
            }
        };
    let moved_keys = image.values.len();
    let durable_effects = image.applied.len();
    let (left_range, right_range) = image.range.split(at);
    let (left_image, right_image) = image.partition(left_range, right_range);

    let mut mounted = Vec::new();
    let staged = async {
        faults.fail(FailurePoint::FirstMount)?;
        let left = mount(&shards, left_range, epochs.0, "green", left_image).await?;
        mounted.push(left.clone());

        faults.fail(FailurePoint::SecondMount)?;
        let right = mount(&shards, right_range, epochs.1, "green", right_image).await?;
        mounted.push(right.clone());

        faults.fail(FailurePoint::BeforeCutover)?;
        let (snapshot, cutover_reconciled) = cutover(
            &directory,
            operation_id.clone(),
            vec![source_id.clone()],
            vec![left.endpoint.clone(), right.endpoint.clone()],
            faults.take(FailurePoint::CutoverReplyLost),
        )
        .await?;
        Ok::<_, String>((left, right, snapshot, cutover_reconciled))
    }
    .await;

    let (left, right, snapshot, cutover_reconciled) = match staged {
        Ok(staged) => staged,
        Err(error) => {
            return Err(abort_precommit(&shards, &source, &operation_id, &mounted, error).await);
        }
    };
    let successors = vec![
        left.endpoint.view.shard_id.clone(),
        right.endpoint.view.shard_id.clone(),
    ];
    let retirement = retire_committed(&shards, &source, &faults).await;

    Ok(TransitionOutcome {
        removed: vec![source_id.clone()],
        members: vec![left, right],
        directory: snapshot,
        report: Some(TransitionReport {
            change: PlannedChange::Split,
            sources: vec![source_id],
            successors,
            moved_keys,
            durable_effects,
            buffered_requests: 0,
            recovered_crash,
            cutover_reconciled,
            retirement_reconciled: retirement.reconciled,
            retirement_pending: retirement.pending,
            source_restart: restart,
        }),
    })
}

async fn reload(
    transition: TransitionContext,
    source: Member,
    config: ShardConfig,
    epoch: u64,
    crash_during_handoff: bool,
) -> Result<TransitionOutcome, String> {
    let TransitionContext {
        shards,
        directory,
        gate,
        faults,
    } = transition;
    let source_id = source.endpoint.view.shard_id.clone();
    let operation_id = format!("reload-{source_id}-to-r{}-e{epoch}", config.revision);
    let (mut image, restart, recovered_crash) =
        match prepare(&source, operation_id.clone(), crash_during_handoff, &gate).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = source.durable.abort_handoff(&operation_id);
                return Err(error);
            }
        };
    let moved_keys = image.values.len();
    let durable_effects = image.applied.len();
    image.config = config;

    let mut mounted = Vec::new();
    let staged = async {
        faults.fail(FailurePoint::FirstMount)?;
        let successor = mount(&shards, image.range, epoch, "blue", image).await?;
        mounted.push(successor.clone());
        let successor_id = successor.endpoint.view.shard_id.clone();

        faults.fail(FailurePoint::BeforeCutover)?;
        let (snapshot, cutover_reconciled) = cutover(
            &directory,
            operation_id.clone(),
            vec![source_id.clone()],
            vec![successor.endpoint.clone()],
            faults.take(FailurePoint::CutoverReplyLost),
        )
        .await?;
        Ok::<_, String>((successor, successor_id, snapshot, cutover_reconciled))
    }
    .await;

    let (successor, successor_id, snapshot, cutover_reconciled) = match staged {
        Ok(staged) => staged,
        Err(error) => {
            return Err(abort_precommit(&shards, &source, &operation_id, &mounted, error).await);
        }
    };
    let retirement = retire_committed(&shards, &source, &faults).await;

    Ok(TransitionOutcome {
        removed: vec![source_id.clone()],
        members: vec![successor],
        directory: snapshot,
        report: Some(TransitionReport {
            change: PlannedChange::ConfigReload,
            sources: vec![source_id],
            successors: vec![successor_id],
            moved_keys,
            durable_effects,
            buffered_requests: 0,
            recovered_crash,
            cutover_reconciled,
            retirement_reconciled: retirement.reconciled,
            retirement_pending: retirement.pending,
            source_restart: restart,
        }),
    })
}

async fn prepare(
    source: &Member,
    handoff_id: String,
    crash_once: bool,
    gate: &TransitionGate,
) -> Result<(DurableImage, RestartEvidence, bool), String> {
    let deadline = Instant::now() + HANDOFF_BOUND;
    let starts_before = source.durable.starts();
    let mut recovered_crash = false;
    let mut waited_for_recovery = false;
    let image = loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| "handoff recovery deadline elapsed".to_owned())?;
        match source
            .endpoint
            .shard
            .call(
                |reply| ShardMsg::PrepareHandoff {
                    handoff_id: handoff_id.clone(),
                    crash_once,
                    reply,
                },
                remaining,
            )
            .await
        {
            Ok(Ok(image)) => break image,
            Ok(Err(error)) => return Err(format!("handoff preparation failed: {error}")),
            Err(CallError::ReplyDropped { .. }) if crash_once => {
                recovered_crash = true;
                if !waited_for_recovery {
                    source
                        .durable
                        .wait_for_start_after(starts_before, deadline)
                        .await?;
                    gate.pause_recovery_if_armed().await;
                    waited_for_recovery = true;
                }
                tokio::task::yield_now().await;
            }
            Err(CallError::ResponseTimedOut { .. }) => {
                break source
                    .durable
                    .wait_for_prepared(&handoff_id, deadline)
                    .await
                    .map_err(|error| {
                        format!("handoff response timed out and reconciliation failed: {error}")
                    })?;
            }
            Err(error) => return Err(format!("handoff preparation failed: {error}")),
        }
    };

    let snapshot = source.scope.snapshot();
    let child = snapshot
        .child("store")
        .ok_or_else(|| "source shard snapshot lost its store actor".to_owned())?;
    Ok((
        image,
        RestartEvidence {
            shard_id: source.endpoint.view.shard_id.clone(),
            generation: child.generation,
            child_restarts: child.restart_count,
            scope_restarts: snapshot.total_restarts,
            actor_starts: source.durable.starts(),
        },
        recovered_crash,
    ))
}

struct RetirementStatus {
    reconciled: bool,
    pending: bool,
}

async fn abort_precommit(
    shards: &DynamicScopeRef,
    source: &Member,
    handoff_id: &str,
    mounted: &[Member],
    cause: String,
) -> String {
    let mut cleanup_errors = Vec::new();
    for member in mounted.iter().rev() {
        if let Err(error) = remove_member_reconciled(shards, member).await {
            cleanup_errors.push(error);
        }
    }
    if let Err(error) = source.durable.abort_handoff(handoff_id) {
        cleanup_errors.push(error);
    }
    if cleanup_errors.is_empty() {
        cause
    } else {
        format!("{cause}; rollback errors: {}", cleanup_errors.join("; "))
    }
}

async fn retire_committed(
    shards: &DynamicScopeRef,
    source: &Member,
    faults: &FaultInjector,
) -> RetirementStatus {
    let first = if faults.take(FailurePoint::BeforeRetire) {
        Err("injected retirement failure before apply".to_owned())
    } else {
        let result = remove_member_once(shards, source).await;
        if result.is_ok() && faults.take(FailurePoint::RetireReplyLost) {
            Err("injected lost retirement reply after apply".to_owned())
        } else {
            result
        }
    };

    if first.is_ok() {
        return RetirementStatus {
            reconciled: false,
            pending: false,
        };
    }
    if !member_is_present(shards, source) {
        return RetirementStatus {
            reconciled: true,
            pending: false,
        };
    }
    let retry = remove_member_once(shards, source).await;
    RetirementStatus {
        reconciled: retry.is_ok() || !member_is_present(shards, source),
        pending: retry.is_err() && member_is_present(shards, source),
    }
}

async fn remove_member_reconciled(shards: &DynamicScopeRef, member: &Member) -> Result<(), String> {
    let first = remove_member_once(shards, member).await;
    if first.is_ok() || !member_is_present(shards, member) {
        return Ok(());
    }
    remove_member_once(shards, member).await.or_else(|error| {
        if member_is_present(shards, member) {
            Err(error)
        } else {
            Ok(())
        }
    })
}

async fn remove_member_once(shards: &DynamicScopeRef, member: &Member) -> Result<(), String> {
    tokio::time::timeout(PHASE_BOUND, shards.remove(&member.scope))
        .await
        .map_err(|_| {
            format!(
                "retirement of {} timed out with an unknown outcome",
                member.endpoint.view.shard_id
            )
        })?
        .map_err(|error| error.to_string())
}

fn member_is_present(shards: &DynamicScopeRef, member: &Member) -> bool {
    shards
        .snapshot()
        .child(&member.endpoint.view.shard_id)
        .is_some()
}

async fn mount(
    shards: &DynamicScopeRef,
    range: KeyRange,
    epoch: u64,
    color: &str,
    image: DurableImage,
) -> Result<Member, String> {
    let id = format!("shard-{:03}-{:03}-{color}-e{epoch}", range.start, range.end);
    let config = image.config;
    let durable = DurableShard::new(image);
    let spec = ActorSpec::new("store", {
        let id = id.clone();
        let durable = Arc::clone(&durable);
        move || Shard::new(id.clone(), epoch, Arc::clone(&durable))
    })
    .restart(RestartPolicy::on_failure().limit(3, Duration::from_secs(10)));
    let shard = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let scope = shards
        .add_subtree(id.clone(), tree)
        .await
        .map_err(|error| error.to_string())?;
    let member = Member {
        endpoint: Endpoint {
            view: RouteView {
                shard_id: id.clone(),
                epoch,
                range,
                config,
            },
            shard,
        },
        scope,
        durable,
    };
    match tokio::time::timeout(PHASE_BOUND, member.scope.wait_started()).await {
        Ok(Ok(())) => Ok(member),
        Ok(Err(error)) => {
            let cleanup = remove_member_reconciled(shards, &member).await;
            Err(format!(
                "startup of {id} failed: {error}; cleanup: {cleanup:?}"
            ))
        }
        Err(error) => {
            let cleanup = remove_member_reconciled(shards, &member).await;
            Err(format!(
                "startup of {id} timed out: {error}; cleanup: {cleanup:?}"
            ))
        }
    }
}

async fn cutover(
    directory: &ActorRef<DirectoryMsg>,
    operation_id: String,
    remove: Vec<String>,
    insert: Vec<Endpoint>,
    simulate_reply_lost: bool,
) -> Result<(DirectorySnapshot, bool), String> {
    let result = directory
        .call(
            |reply| DirectoryMsg::Cutover {
                operation_id: operation_id.clone(),
                remove,
                insert,
                reply,
            },
            CALL_BOUND,
        )
        .await;
    match result {
        Ok(Ok(snapshot)) if !simulate_reply_lost => Ok((snapshot, false)),
        Ok(Err(error)) => Err(error),
        Ok(Ok(_)) | Err(CallError::ResponseTimedOut { .. }) => {
            let snapshot = directory
                .call(
                    |reply| DirectoryMsg::CutoverStatus {
                        operation_id,
                        reply,
                    },
                    CALL_BOUND,
                )
                .await
                .map_err(|error| format!("directory cutover reconciliation failed: {error}"))?
                .ok_or_else(|| "directory cutover outcome remained unknown".to_owned())?;
            Ok((snapshot, true))
        }
        Err(error) => Err(error.to_string()),
    }
}
