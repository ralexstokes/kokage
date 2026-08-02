use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorRef, ActorSpec, CallError, Context, DynamicScopeRef, ExitResult, Reply,
    RestartPolicy, ScopeRef, Tree,
};
use tokio::sync::Notify;

use crate::{
    directory::{DirectoryMsg, Endpoint},
    model::{
        DirectorySnapshot, DurableImage, Key, KeyRange, PlannedChange, ReadReceipt,
        RestartEvidence, RouteView, ShardConfig, TransitionReport, Write, WriteReceipt,
    },
    shard::{DurableShard, Shard, ShardMsg},
};

const CALL_BOUND: Duration = Duration::from_secs(2);
const TRANSITION_BOUND: Duration = Duration::from_secs(6);

#[derive(Clone, Default)]
pub struct TransitionGate {
    held: Arc<AtomicBool>,
    entries: Arc<AtomicU64>,
    entered: Arc<Notify>,
    released: Arc<Notify>,
    buffered: Arc<AtomicU64>,
    buffered_changed: Arc<Notify>,
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
}

#[derive(Clone)]
struct Member {
    endpoint: Endpoint,
    scope: ScopeRef,
    durable: Arc<DurableShard>,
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
    members: Vec<Member>,
    pending: Option<Pending>,
    next_epoch: u64,
}

impl MembershipRouter {
    pub fn new(
        shards: DynamicScopeRef,
        directory: ActorRef<DirectoryMsg>,
        gate: TransitionGate,
    ) -> Self {
        Self {
            shards,
            directory,
            gate,
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
        let shards = self.shards.clone();
        let directory = self.directory.clone();
        let gate = self.gate.clone();
        self.pending = Some(Pending {
            ranges: vec![source.endpoint.view.range],
            buffered: Vec::new(),
            reply: PendingReply::Change(reply),
        });
        ctx.offload(
            TRANSITION_BOUND,
            async move {
                gate.pause_if_armed().await;
                split(shards, directory, source, at, epochs).await
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
        let shards = self.shards.clone();
        let directory = self.directory.clone();
        let gate = self.gate.clone();
        self.pending = Some(Pending {
            ranges: vec![source.endpoint.view.range],
            buffered: Vec::new(),
            reply: PendingReply::Change(reply),
        });
        ctx.offload(
            TRANSITION_BOUND,
            async move {
                gate.pause_if_armed().await;
                reload(
                    shards,
                    directory,
                    source,
                    config,
                    epoch,
                    crash_during_handoff,
                )
                .await
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
    let snapshot = cutover(&directory, Vec::new(), vec![member.endpoint.clone()]).await?;
    Ok(TransitionOutcome {
        removed: Vec::new(),
        members: vec![member],
        directory: snapshot,
        report: None,
    })
}

async fn split(
    shards: DynamicScopeRef,
    directory: ActorRef<DirectoryMsg>,
    source: Member,
    at: Key,
    epochs: (u64, u64),
) -> Result<TransitionOutcome, String> {
    let source_id = source.endpoint.view.shard_id.clone();
    let handoff_id = format!("split-{source_id}-at-{at}");
    let (image, restart, recovered_crash) = prepare(&source, handoff_id, false).await?;
    let moved_keys = image.values.len();
    let durable_effects = image.applied.len();
    let (left_range, right_range) = image.range.split(at);
    let (left_image, right_image) = image.partition(left_range, right_range);

    let left = mount(&shards, left_range, epochs.0, "green", left_image).await?;
    let right = mount(&shards, right_range, epochs.1, "green", right_image).await?;
    let successors = vec![
        left.endpoint.view.shard_id.clone(),
        right.endpoint.view.shard_id.clone(),
    ];
    let snapshot = cutover(
        &directory,
        vec![source_id.clone()],
        vec![left.endpoint.clone(), right.endpoint.clone()],
    )
    .await?;
    shards
        .remove(&source.scope)
        .await
        .map_err(|error| error.to_string())?;

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
            source_restart: restart,
        }),
    })
}

async fn reload(
    shards: DynamicScopeRef,
    directory: ActorRef<DirectoryMsg>,
    source: Member,
    config: ShardConfig,
    epoch: u64,
    crash_during_handoff: bool,
) -> Result<TransitionOutcome, String> {
    let source_id = source.endpoint.view.shard_id.clone();
    let handoff_id = format!("reload-{source_id}-to-r{}", config.revision);
    let (mut image, restart, recovered_crash) =
        prepare(&source, handoff_id, crash_during_handoff).await?;
    let moved_keys = image.values.len();
    let durable_effects = image.applied.len();
    image.config = config;
    let successor = mount(&shards, image.range, epoch, "blue", image).await?;
    let successor_id = successor.endpoint.view.shard_id.clone();
    let snapshot = cutover(
        &directory,
        vec![source_id.clone()],
        vec![successor.endpoint.clone()],
    )
    .await?;
    shards
        .remove(&source.scope)
        .await
        .map_err(|error| error.to_string())?;

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
            source_restart: restart,
        }),
    })
}

async fn prepare(
    source: &Member,
    handoff_id: String,
    crash_once: bool,
) -> Result<(DurableImage, RestartEvidence, bool), String> {
    let first = source
        .endpoint
        .shard
        .call(
            |reply| ShardMsg::PrepareHandoff {
                handoff_id: handoff_id.clone(),
                crash_once,
                reply,
            },
            CALL_BOUND,
        )
        .await;
    let (image, recovered_crash) = match first {
        Ok(image) => (image, false),
        Err(CallError::ReplyDropped { .. }) if crash_once => {
            let recovered = source
                .endpoint
                .shard
                .call(
                    |reply| ShardMsg::PrepareHandoff {
                        handoff_id,
                        crash_once,
                        reply,
                    },
                    CALL_BOUND,
                )
                .await
                .map_err(|error| format!("handoff recovery failed: {error}"))?;
            (recovered, true)
        }
        Err(error) => return Err(format!("handoff preparation failed: {error}")),
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
    scope
        .wait_started()
        .await
        .map_err(|error| error.to_string())?;
    Ok(Member {
        endpoint: Endpoint {
            view: RouteView {
                shard_id: id,
                epoch,
                range,
                config,
            },
            shard,
        },
        scope,
        durable,
    })
}

async fn cutover(
    directory: &ActorRef<DirectoryMsg>,
    remove: Vec<String>,
    insert: Vec<Endpoint>,
) -> Result<DirectorySnapshot, String> {
    directory
        .call(
            |reply| DirectoryMsg::Cutover {
                remove,
                insert,
                reply,
            },
            CALL_BOUND,
        )
        .await
        .map_err(|error| error.to_string())?
}
