//! An "operator" actor that inspects and controls every other part of its tree.
//!
//! The operator sits directly in the root scope, so [`Context::scope`] hands it
//! the root [`ScopeRef`] — the full observation-and-control capability for the
//! entire tree — without any extra wiring. From there it can:
//!
//! - render the recursive [`SupervisorSnapshot`] and per-actor stats,
//! - pump the tree-wide lifecycle stream into its own mailbox,
//! - peer-monitor individual actors across restarts,
//! - navigate to nested scopes with [`ScopeRef::subtree`],
//! - grow and shrink the dynamic `jobs` scope at runtime,
//! - stop a subtree, and finally bring the whole tree down.
//!
//! Three limits shape the pattern. There is no per-child kill/restart/pause
//! primitive: restarting is policy-driven, and the only forced stops are
//! scope shutdown and dynamic removal, so ordered members cannot be removed
//! individually. Message-level access stays typed: snapshots carry ids and
//! states but no senders, so messaging an arbitrary actor requires holding
//! its [`ActorRef`] (wired at build time, or via a userland registry as in
//! `examples/directory.rs`). And there are no privilege levels: anything
//! holding the root [`ScopeRef`] has this same power, which is why the
//! operator is a role, not an authority the framework enforces.

use std::{error::Error, fmt::Write as _, io, time::Duration};

use kokage::{
    BoxError, MonitorEvent, RestartPolicy, SubtreeSpec,
    observe::{ChildStateView, LifecycleEvent, LifecycleEventKind, SupervisorSnapshot},
    prelude::*,
};

// --- the supervised application: a static `pipeline` subtree ---

struct Ingest {
    worker: ActorRef<String>,
}

impl Actor for Ingest {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.worker.send(message).await?;
        Ok(())
    }
}

struct Worker;

impl Actor for Worker {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if message == "poison" {
            return Err::<_, BoxError>(Box::new(io::Error::other("simulated worker failure")));
        }
        println!("[worker] processed `{message}`");
        Ok(())
    }
}

// --- workloads the operator manages dynamically in the `jobs` scope ---

struct Job {
    id: String,
}

impl Actor for Job {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("[job {}] started", self.id);
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

// --- the operator actor ---

enum OperatorMsg {
    /// Render the whole tree: recursive snapshot plus per-actor stats.
    Report(Reply<String>),
    /// Add a `Job` actor to the dynamic `jobs` scope.
    SpawnJob(String, Reply<Result<(), String>>),
    /// Run finite one-shot work under supervision in `jobs`.
    RunOnce(String, Reply<Result<(), String>>),
    /// Remove whichever `jobs` member currently owns the id.
    RemoveJob(String, Reply<Result<(), String>>),
    /// Gracefully stop the `pipeline` subtree and wait for it.
    StopPipeline(Reply<Result<(), String>>),
    /// Request graceful shutdown of the entire tree.
    ShutdownAll,
    /// Tree-wide lifecycle transitions pumped from the root scope.
    TreeEvent(LifecycleEvent),
    /// Peer-monitor transitions for the watched worker.
    WorkerEvent(MonitorEvent),
}

struct Operator {
    worker: ActorRef<String>,
    /// Owns the lifecycle pump; dropping it stops tree-event delivery.
    tree_watch: Option<Guard>,
}

impl Actor for Operator {
    type Msg = OperatorMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        // The operator is a direct child of the root scope, so its enclosing
        // scope *is* the root of the whole tree.
        let root = ctx.scope();
        self.tree_watch = Some(
            root.subscribe_lifecycle()
                .forward_to(&ctx.myself(), OperatorMsg::TreeEvent),
        );
        ctx.watch(&self.worker, OperatorMsg::WorkerEvent);
        Ok(())
    }

    async fn handle(&mut self, message: OperatorMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        let root = ctx.scope();
        match message {
            OperatorMsg::Report(reply) => reply.send(render_report(&root)),
            OperatorMsg::SpawnJob(id, reply) => {
                let result = match jobs_scope(&root) {
                    Ok(jobs) => jobs
                        .add_actor(id.clone(), move || Job { id: id.clone() })
                        .await
                        .map(drop)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                };
                reply.send(result);
            }
            OperatorMsg::RunOnce(id, reply) => {
                let result = match jobs_scope(&root) {
                    Ok(jobs) => jobs
                        .spawn_once(id.clone(), move |_ctx| async move {
                            println!("[once {id}] finished");
                            Ok(())
                        })
                        .await
                        .map(drop)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                };
                reply.send(result);
            }
            OperatorMsg::RemoveJob(id, reply) => {
                let result = match jobs_scope(&root) {
                    Ok(jobs) => jobs
                        .remove_named(id)
                        .await
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                };
                reply.send(result);
            }
            OperatorMsg::StopPipeline(reply) => {
                // The pipeline is a sibling scope: it stops while the operator
                // stays live, so awaiting here cannot deadlock. It does hold
                // the operator's mailbox for the wait; offload it when the operator
                // must stay responsive.
                let result = match root.subtree("pipeline") {
                    Some(pipeline) => pipeline
                        .shutdown_and_wait()
                        .await
                        .map_err(|error| error.to_string()),
                    None => Err("no `pipeline` subtree".to_owned()),
                };
                reply.send(result);
            }
            OperatorMsg::ShutdownAll => {
                println!("[operator] shutting the tree down");
                ctx.request_scope_shutdown();
            }
            OperatorMsg::TreeEvent(event) => {
                if let LifecycleEventKind::Child(child) = &event.kind {
                    let path: Vec<&str> = event
                        .scope_path
                        .iter()
                        .map(|segment| segment.id.as_str())
                        .collect();
                    println!(
                        "[operator] /{} `{}` {:?}",
                        path.join("/"),
                        child.child_id,
                        child.kind
                    );
                }
            }
            OperatorMsg::WorkerEvent(event) => {
                println!("[operator] watched `{}` {:?}", event.actor_id, event.kind);
            }
        }
        Ok(())
    }
}

/// Navigates from the root to the dynamic `jobs` scope's mutation capability.
fn jobs_scope(root: &ScopeRef) -> Result<DynamicScopeRef, String> {
    root.subtree("jobs")
        .ok_or_else(|| "no `jobs` subtree".to_owned())?
        .dynamic()
        .ok_or_else(|| "`jobs` is not a dynamic scope".to_owned())
}

fn render_report(root: &ScopeRef) -> String {
    let snapshot = root.snapshot();
    let mut out = String::new();
    let _ = writeln!(
        out,
        "root [{:?} {:?}] restarts={}",
        snapshot.kind, snapshot.strategy, snapshot.total_restarts
    );
    render_children(&snapshot, 1, &mut out);
    let _ = writeln!(out, "actor stats:");
    for scoped in root.actor_stats() {
        let mut path: Vec<&str> = scoped
            .scope_path
            .iter()
            .map(|segment| segment.id.as_str())
            .collect();
        path.push(&scoped.stats.actor_id);
        let _ = writeln!(
            out,
            "  /{}: received={} accepted={} mailbox={}/{}",
            path.join("/"),
            scoped.stats.messages_received,
            scoped.stats.messages_accepted,
            scoped.stats.mailbox_depth,
            scoped.stats.mailbox_capacity,
        );
    }
    out
}

fn render_children(snapshot: &SupervisorSnapshot, depth: usize, out: &mut String) {
    for child in &snapshot.children {
        let _ = writeln!(
            out,
            "{}{} [{}] restarts={}",
            "  ".repeat(depth),
            child.id,
            state_label(&child.state),
            child.restart_count,
        );
        if let Some(nested) = &child.supervisor {
            render_children(nested, depth + 1, out);
        }
    }
}

fn state_label(state: &ChildStateView) -> &'static str {
    match state {
        ChildStateView::Starting { .. } => "starting",
        ChildStateView::Running { .. } => "running",
        ChildStateView::Stopping { .. } => "stopping",
        ChildStateView::Stopped { .. } => "stopped",
        ChildStateView::StartupAborted { .. } => "startup-aborted",
        _ => "unknown",
    }
}

const CALL_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // The `pipeline` subtree: worker first, then the ingest that feeds it.
    let mut pipeline = Tree::new();
    let worker_spec = ActorSpec::new("worker", || Worker)
        .restart(RestartPolicy::on_failure().limit(3, Duration::from_secs(10)));
    let worker = worker_spec.actor_ref();
    pipeline.add_actor_spec(worker_spec);
    let ingest = pipeline.add_actor("ingest", {
        let worker = worker.clone();
        move || Ingest {
            worker: worker.clone(),
        }
    });

    let mut tree = Tree::new();
    let operator = tree.add_actor("operator", {
        let worker = worker.clone();
        move || Operator {
            worker: worker.clone(),
            tree_watch: None,
        }
    });
    // `never()` on the subtree edge makes an operator-ordered stop final: the
    // root will not revive the pipeline behind the operator's back.
    tree.add_subtree_spec(
        "pipeline",
        SubtreeSpec::from(pipeline).restart(RestartPolicy::never()),
    );
    // An initially empty dynamic scope the operator populates at runtime.
    tree.add_subtree("jobs", DynamicTree::new());

    let running_tree = tree.spawn()?;
    let scope = running_tree.scope();
    scope.wait_started().await?;

    // 1. Inspect the freshly started tree.
    println!("--- initial report ---");
    print!(
        "{}",
        operator.call(OperatorMsg::Report, CALL_TIMEOUT).await?
    );

    // 2. Grow the dynamic `jobs` scope: a long-lived actor and one-shot work.
    operator
        .call(
            |reply| OperatorMsg::SpawnJob("job-1".into(), reply),
            CALL_TIMEOUT,
        )
        .await??;
    operator
        .call(
            |reply| OperatorMsg::RunOnce("backfill".into(), reply),
            CALL_TIMEOUT,
        )
        .await??;

    // 3. Fail the worker and watch supervision restart it. The operator reports
    //    the same transitions through its lifecycle pump and peer watch.
    let mut snapshots = scope.subscribe_snapshots();
    let baseline = scope
        .snapshot()
        .descendant(["pipeline", "worker"])
        .expect("worker is supervised")
        .generation;
    ingest.send("one".into()).await?;
    ingest.send("poison".into()).await?;
    snapshots
        .wait_for(|snapshot| {
            snapshot
                .descendant(["pipeline", "worker"])
                .is_some_and(|child| child.generation > baseline && child.state.is_running())
        })
        .await?;
    ingest.send("two".into()).await?;

    println!("--- after failure and dynamic growth ---");
    print!(
        "{}",
        operator.call(OperatorMsg::Report, CALL_TIMEOUT).await?
    );

    // 4. Shrink and stop: remove the dynamic job, then stop the pipeline.
    operator
        .call(
            |reply| OperatorMsg::RemoveJob("job-1".into(), reply),
            CALL_TIMEOUT,
        )
        .await??;
    operator
        .call(OperatorMsg::StopPipeline, CALL_TIMEOUT)
        .await??;

    println!("--- after removing job-1 and stopping the pipeline ---");
    print!(
        "{}",
        operator.call(OperatorMsg::Report, CALL_TIMEOUT).await?
    );

    // 5. The operator brings the whole tree down; the owner just waits.
    operator.send(OperatorMsg::ShutdownAll).await?;
    running_tree.wait().await?;
    Ok(())
}
