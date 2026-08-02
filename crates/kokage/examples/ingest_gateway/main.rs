//! A deterministic loopback telemetry gateway and acceptance script.
//!
//! ```text
//! ingest-gateway (ordered OneForOne)
//! ├── enricher       actor, bounded FIFO (4)
//! ├── batcher        actor, bounded FIFO (2)
//! ├── shipper        actor, bounded FIFO (1), jittered exponential restart
//! ├── connections    dynamic scope
//! │   └── connection-N  one-shot raw actor owning one TcpStream
//! └── listener       readiness-gated supervised loopback accept task
//! ```
//!
//! Scripted clients send length-prefixed JSON over real TCP. The script holds
//! the sink, fills every FIFO from the far end back to ingress, and then
//! asserts exact `ActorStats` while `try_send` sheds only `Full` frames. A bad
//! client fails only its connection actor. Run with `--console` to keep the
//! verified tree attached to `kokage-console` until Ctrl-C.

mod model;
mod network;
mod pipeline;

use std::{
    error::Error,
    sync::{Arc, Mutex, PoisonError, atomic::Ordering},
    time::Duration,
};

use kokage::{
    ActorRef, ActorSpec, Backoff, DynamicScopeRef, DynamicTree, Mailbox, RestartPolicy, ScopeRef,
    Strategy, Tree,
    observe::{ActorStats, ChildEventKind, LifecycleEvent, LifecycleEventKind, LifecycleWatch},
};
use kokage_console::{ConsoleBuilder, ConsoleHandle};
use tokio::{net::TcpStream, sync::watch, time::timeout};

use model::{Evidence, GatewayReport, PipelineGate, TelemetryEvent};
use pipeline::{BatchMsg, Batcher, Enricher, ScriptedSink, ShipBatch};

const ACCEPTANCE_BOUND: Duration = Duration::from_secs(5);
const CALL_BOUND: Duration = Duration::from_secs(1);
const ENRICHER_CAPACITY: usize = 4;
const BATCHER_CAPACITY: usize = 2;
const SHIPPER_CAPACITY: usize = 1;
const SINK_RESTART_LIMIT: usize = 4;
const SINK_RESTART_WINDOW: Duration = Duration::from_secs(2);
const BACKOFF_BASE: Duration = Duration::from_millis(40);

type AnyError = Box<dyn Error + Send + Sync>;

struct Gateway {
    running: kokage::RunningTree,
    root: ScopeRef,
    connections: DynamicScopeRef,
    enricher: ActorRef<TelemetryEvent>,
    batcher: ActorRef<BatchMsg>,
    shipper: ActorRef<ShipBatch>,
    address: watch::Receiver<Option<std::net::SocketAddr>>,
    evidence: Evidence,
    gate: PipelineGate,
    lifecycle: Option<LifecycleWatch>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LifecycleReport {
    sink_failed_exits: u64,
    sink_failed_generations: Vec<u64>,
    sink_started_generations: Vec<u64>,
    sink_restart_generations: Vec<u64>,
    sink_restart_delays: Vec<Duration>,
    connection_failed_exits: u64,
    connection_removals: u64,
    lagged_events: u64,
}

#[derive(Clone, Default)]
struct LifecycleEvidence(Arc<Mutex<LifecycleReport>>);

impl LifecycleEvidence {
    fn record(&self, event: &LifecycleEvent) {
        let mut report = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match &event.kind {
            LifecycleEventKind::Child(child)
                if event.scope_path.is_empty() && child.child_id == "shipper" =>
            {
                match &child.kind {
                    ChildEventKind::Started { generation } => {
                        report.sink_started_generations.push(*generation);
                    }
                    ChildEventKind::Exited { generation, exit } if exit.is_failure() => {
                        report.sink_failed_exits += 1;
                        report.sink_failed_generations.push(*generation);
                    }
                    ChildEventKind::RestartScheduled { generation, delay } => {
                        report.sink_restart_generations.push(*generation);
                        report.sink_restart_delays.push(*delay);
                    }
                    _ => {}
                }
            }
            LifecycleEventKind::Child(child)
                if event.scope_path.len() == 1
                    && event.scope_path[0].id == "connections"
                    && child.child_id.starts_with("connection-") =>
            {
                match &child.kind {
                    ChildEventKind::Exited { exit, .. } if exit.is_failure() => {
                        report.connection_failed_exits += 1;
                    }
                    ChildEventKind::Removed => report.connection_removals += 1,
                    _ => {}
                }
            }
            LifecycleEventKind::Lagged { dropped } => report.lagged_events += dropped,
            _ => {}
        }
    }

    fn snapshot(&self) -> LifecycleReport {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;
    let console_enabled = std::env::args()
        .skip(1)
        .any(|argument| argument == "--console");

    let mut gateway = assemble()?;
    let mut lifecycle = gateway
        .lifecycle
        .take()
        .expect("assembly retains the pre-spawn lifecycle watch");
    let lifecycle_evidence = LifecycleEvidence::default();
    let lifecycle_for_collector = lifecycle_evidence.clone();
    let lifecycle_collector = tokio::spawn(async move {
        while let Some(event) = lifecycle.next().await {
            lifecycle_for_collector.record(&event);
            if event.scope_path.is_empty()
                && matches!(event.kind, LifecycleEventKind::SupervisorStopped)
            {
                break;
            }
        }
    });

    timeout(ACCEPTANCE_BOUND, gateway.root.wait_started()).await??;
    let sink = gateway
        .root
        .snapshot()
        .child("shipper")
        .cloned()
        .ok_or("shipper snapshot missing")?;
    assert_eq!(sink.generation, 2);
    assert_eq!(sink.restart_count, 2);
    assert!(
        sink.state
            .last_exit()
            .and_then(kokage::observe::ExitStatus::failure_message)
            .is_some_and(|message| message.contains("shipper"))
    );
    gateway
        .evidence
        .wait_for(|report| report.sink_connect_attempts == 3)
        .await;
    println!("PHASE 1 OK — flaky sink recovered after two supervised backoffs");

    let console = attach_console(console_enabled, &gateway.root).await?;
    let address = wait_for_address(gateway.address.clone()).await?;

    let mut flood = TcpStream::connect(address).await?;
    flood.set_nodelay(true)?;
    for id in 0..=1 {
        network::write_event(&mut flood, &TelemetryEvent::scripted(id)).await?;
    }
    timeout(ACCEPTANCE_BOUND, gateway.gate.wait_entered()).await?;

    for id in 2..=3 {
        network::write_event(&mut flood, &TelemetryEvent::scripted(id)).await?;
    }
    wait_stats(&gateway.shipper, |stats| {
        stats.messages_accepted == 2 && stats.messages_received == 1 && stats.mailbox_depth == 1
    })
    .await?;

    for id in 4..=5 {
        network::write_event(&mut flood, &TelemetryEvent::scripted(id)).await?;
    }
    wait_stats(&gateway.batcher, |stats| stats.messages_received == 6).await?;

    for id in 6..=7 {
        network::write_event(&mut flood, &TelemetryEvent::scripted(id)).await?;
    }
    wait_stats(&gateway.batcher, |stats| {
        stats.mailbox_depth == BATCHER_CAPACITY
    })
    .await?;

    network::write_event(&mut flood, &TelemetryEvent::scripted(8)).await?;
    wait_stats(&gateway.enricher, |stats| stats.messages_received == 9).await?;
    for id in 9..=12 {
        network::write_event(&mut flood, &TelemetryEvent::scripted(id)).await?;
    }
    wait_stats(&gateway.enricher, |stats| {
        stats.mailbox_depth == ENRICHER_CAPACITY
    })
    .await?;

    for id in 13..=20 {
        network::write_event(&mut flood, &TelemetryEvent::scripted(id)).await?;
    }
    let overload = timeout(
        ACCEPTANCE_BOUND,
        gateway
            .evidence
            .wait_for(|report| report.valid_frames == 21),
    )
    .await?;
    assert_overload_stats(&gateway, &overload);
    println!(
        "PHASE 2 OK — end-to-end backpressure shed {} of {} valid frames at ingress",
        overload.frames_shed_full, overload.valid_frames
    );

    gateway.gate.open();
    wait_stats(&gateway.batcher, |stats| stats.messages_received == 13).await?;
    let flushed = gateway
        .batcher
        .call(|reply| BatchMsg::Flush { reply }, CALL_BOUND)
        .await?;
    assert_eq!(flushed, 1);
    timeout(
        ACCEPTANCE_BOUND,
        gateway
            .evidence
            .wait_for(|report| report.shipped_ids.len() == 13),
    )
    .await?;
    drop(flood);

    let mut malformed = TcpStream::connect(address).await?;
    network::write_malformed(&mut malformed).await?;
    drop(malformed);
    timeout(
        ACCEPTANCE_BOUND,
        gateway
            .evidence
            .wait_for(|report| report.malformed_clients == 1),
    )
    .await?;

    let mut healthy = TcpStream::connect(address).await?;
    for id in 100..=101 {
        network::write_event(&mut healthy, &TelemetryEvent::scripted(id)).await?;
    }
    drop(healthy);
    timeout(
        ACCEPTANCE_BOUND,
        gateway
            .evidence
            .wait_for(|report| report.shipped_ids.len() == 15),
    )
    .await?;
    wait_for_empty_scope(&gateway.connections).await?;
    println!("PHASE 3 OK — malformed client failed alone; healthy peer still shipped");

    let final_report = gateway.evidence.snapshot();
    assert_report(&final_report);
    let scoped_stats = gateway.root.actor_stats();
    assert_eq!(scoped_stats.len(), 3, "connection actors were torn down");
    assert!(
        scoped_stats
            .iter()
            .all(|sample| sample.scope_path.is_empty())
    );
    println!("PHASE 4 OK — overload report and cumulative ActorStats agree");

    if let Some(console) = console {
        println!("acceptance complete; press Ctrl-C to stop the attached console");
        tokio::signal::ctrl_c().await?;
        console.shutdown_and_wait().await?;
    }

    gateway.running.shutdown().await?;
    timeout(ACCEPTANCE_BOUND, lifecycle_collector).await??;
    let lifecycle_report = lifecycle_evidence.snapshot();
    assert_lifecycle(&lifecycle_report);
    println!("PHASE 5 OK — lifecycle report observed backoff, isolation, and teardown");
    Ok(())
}

fn assemble() -> Result<Gateway, AnyError> {
    let evidence = Evidence::default();
    let gate = PipelineGate::default();
    let attempts = pipeline::shared_attempt_counter();

    let shipper_spec = ActorSpec::new("shipper", {
        let evidence = evidence.clone();
        let gate = gate.clone();
        move || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            ScriptedSink::new(attempt, evidence.clone(), gate.clone())
        }
    })
    .mailbox(Mailbox::queue(SHIPPER_CAPACITY))
    .message_size(pipeline::ship_batch_size)
    .restart(
        RestartPolicy::on_failure()
            .limit(SINK_RESTART_LIMIT, SINK_RESTART_WINDOW)
            .backoff(Backoff::exponential_with_jitter(
                BACKOFF_BASE,
                2,
                Duration::from_millis(200),
            )),
    );
    let shipper = shipper_spec.actor_ref();

    let batcher_spec = ActorSpec::new("batcher", {
        let shipper = shipper.clone();
        move || Batcher::new(shipper.clone())
    })
    .mailbox(Mailbox::queue(BATCHER_CAPACITY));
    let batcher = batcher_spec.actor_ref();

    let enricher_spec = ActorSpec::new("enricher", {
        let batcher = batcher.clone();
        move || Enricher::new(batcher.clone())
    })
    .mailbox(Mailbox::queue(ENRICHER_CAPACITY))
    .message_size(pipeline::event_size);
    let enricher = enricher_spec.actor_ref();

    let connections_tree = DynamicTree::new();
    let connections = connections_tree.scope();
    let (address_tx, address) = watch::channel(None);

    let mut tree = Tree::new().strategy(Strategy::OneForOne);
    tree.add_actor_spec(enricher_spec);
    tree.add_actor_spec(batcher_spec);
    tree.add_actor_spec(shipper_spec);
    tree.add_subtree("connections", connections_tree);
    tree.add_task_spec(network::listener(
        connections.clone(),
        enricher.clone(),
        evidence.clone(),
        address_tx,
    ));
    let root = tree.scope();
    let lifecycle = root.subscribe_lifecycle();
    let running = tree.spawn()?;

    Ok(Gateway {
        running,
        root,
        connections,
        enricher,
        batcher,
        shipper,
        address,
        evidence,
        gate,
        lifecycle: Some(lifecycle),
    })
}

async fn attach_console(enabled: bool, root: &ScopeRef) -> Result<Option<ConsoleHandle>, AnyError> {
    if !enabled {
        return Ok(None);
    }
    let console = ConsoleBuilder::for_runtime(root)
        .bind(([127, 0, 0, 1], 0))
        .spawn()
        .await?;
    println!("console available at http://{}", console.local_addr());
    Ok(Some(console))
}

async fn wait_for_address(
    mut address: watch::Receiver<Option<std::net::SocketAddr>>,
) -> Result<std::net::SocketAddr, AnyError> {
    timeout(ACCEPTANCE_BOUND, async {
        loop {
            if let Some(address) = *address.borrow() {
                return Ok(address);
            }
            address.changed().await?;
        }
    })
    .await?
}

async fn wait_stats<M: Send + 'static>(
    actor: &ActorRef<M>,
    predicate: impl Fn(&ActorStats) -> bool,
) -> Result<ActorStats, AnyError> {
    timeout(ACCEPTANCE_BOUND, async {
        loop {
            let stats = actor.stats();
            if predicate(&stats) {
                return stats;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(Into::into)
}

async fn wait_for_empty_scope(scope: &DynamicScopeRef) -> Result<(), AnyError> {
    let mut snapshots = scope.subscribe_snapshots();
    timeout(
        ACCEPTANCE_BOUND,
        snapshots.wait_for(|snapshot| snapshot.children.is_empty()),
    )
    .await??;
    Ok(())
}

fn assert_overload_stats(gateway: &Gateway, report: &GatewayReport) {
    assert_eq!(report.frames_accepted, 13, "{report:?}");
    assert_eq!(report.frames_shed_full, 8, "{report:?}");
    assert_eq!(report.degraded_connections, 0, "{report:?}");

    let enricher = gateway.enricher.stats();
    assert_eq!(enricher.messages_accepted, 13);
    assert_eq!(enricher.messages_received, 9);
    assert_eq!(enricher.sends_rejected, 8);
    assert_eq!(enricher.mailbox_depth, ENRICHER_CAPACITY);
    assert_eq!(enricher.mailbox_capacity, ENRICHER_CAPACITY);

    let batcher = gateway.batcher.stats();
    assert_eq!(batcher.messages_accepted, 8);
    assert_eq!(batcher.messages_received, 6);
    assert_eq!(batcher.mailbox_depth, BATCHER_CAPACITY);
    assert_eq!(batcher.mailbox_capacity, BATCHER_CAPACITY);

    let shipper = gateway.shipper.stats();
    assert_eq!(shipper.messages_accepted, 2);
    assert_eq!(shipper.messages_received, 1);
    assert_eq!(shipper.mailbox_depth, SHIPPER_CAPACITY);
    assert_eq!(shipper.mailbox_capacity, SHIPPER_CAPACITY);
}

fn assert_report(report: &GatewayReport) {
    assert_eq!(report.connections_accepted, 3, "{report:?}");
    assert_eq!(report.clean_disconnects, 2, "{report:?}");
    assert_eq!(report.malformed_clients, 1, "{report:?}");
    assert_eq!(report.valid_frames, 23, "{report:?}");
    assert_eq!(report.frames_accepted, 15, "{report:?}");
    assert_eq!(report.frames_shed_full, 8, "{report:?}");
    assert_eq!(report.degraded_connections, 0, "{report:?}");
    assert_eq!(report.sink_connect_attempts, 3, "{report:?}");
    assert_eq!(report.shipped_batches, 8, "{report:?}");
    assert_eq!(
        report.shipped_ids,
        (0..=12).chain(100..=101).collect::<Vec<_>>()
    );
}

fn assert_lifecycle(report: &LifecycleReport) {
    assert_eq!(report.lagged_events, 0, "{report:?}");
    assert_eq!(report.sink_failed_exits, 2, "{report:?}");
    assert_eq!(report.sink_failed_generations, [0, 1], "{report:?}");
    assert_eq!(report.sink_restart_generations, [0, 1], "{report:?}");
    assert_eq!(report.sink_started_generations, [2], "{report:?}");
    assert_eq!(report.sink_restart_delays.len(), 2, "{report:?}");
    assert!(
        (BACKOFF_BASE / 2..=BACKOFF_BASE).contains(&report.sink_restart_delays[0]),
        "{report:?}"
    );
    assert!(
        (BACKOFF_BASE..=BACKOFF_BASE * 2).contains(&report.sink_restart_delays[1]),
        "{report:?}"
    );
    assert!(
        report.sink_restart_delays[1] >= report.sink_restart_delays[0],
        "{report:?}"
    );
    assert_eq!(report.connection_failed_exits, 1, "{report:?}");
    assert_eq!(report.connection_removals, 3, "{report:?}");
    assert!(report.sink_failed_exits < SINK_RESTART_LIMIT as u64);
}
