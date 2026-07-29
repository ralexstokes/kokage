//! A multi-venue trading engine exercising the library's supervision,
//! messaging, and timer features together.
//!
//! `#[derive(Supervision)]` wires the cyclic core actors and their typed refs;
//! the configured venue slots join the same graph explicitly. Linear actor
//! nodes then build a root scope with a restart-budgeted `venues` subtree.
//! Venue actors capture core refs (ledger, reconciler), while the reconciler
//! captures every feed ref. Graph labels remain qualified as
//! `venues.venue-a-feed` and friends, while supervisor child ids are local.
//!
//! # Modules
//!
//! * `messages` — the shared vocabulary: one message enum per actor, the
//!   outcome types, and `CALL_DEADLINE` for bounded calls.
//! * `venue` — the exchange side: `ExchangeSim`, a shared fake exchange
//!   holding order state and scripted misbehavior; `VenueFeed`, which turns
//!   injected ticks into market snapshots for the reconciler (keyed conflating
//!   mailbox, so market data cannot replace control traffic; simulated parse
//!   delay; scripted crash); and `VenueGateway`,
//!   which handles place/cancel/query/cancel-all and schedules delayed fill
//!   delivery.
//! * `router` — `OrderRouter`, the order front door: assigns order keys and
//!   pipelines deadline-bounded gateway calls off its handle loop (spawned
//!   tasks complete the caller's reply and report back via `SubmitResolved`)
//!   so one stalled venue cannot head-of-line block the others. Intents
//!   whose call timed out stay unknown until `ReconcileAll` re-queries and
//!   re-places them under the same idempotency key.
//! * `ledger` — the record of effects: counts acks/fills/cancellations per
//!   order key and serves a report for assertions. Drains its mailbox at
//!   shutdown so queued effects are not lost.
//! * `reconciler` — market-data health. Tracks each venue as
//!   Fresh/Stale/Down: ticks make it Fresh, a state-timeout-driven sweep
//!   demotes quiet venues, and watches on the feeds map restart and
//!   termination events to Down.
//! * `control` — the kill switch: closes the shared intake gate checked by
//!   the router and fans `CancelAll` out to both gateways.
//! * `health` — the restart circuit breaker: counts venue-subtree restarts
//!   in a sliding window and trips the kill switch at a threshold.
//! * `telemetry` — per-hop latency aggregates, the metrics debugging
//!   recorder, and a background supervisor-snapshot sampler.
//!
//! # Data flow
//!
//! Three loops plus telemetry:
//!
//! ```text
//! market data
//! -----------
//!       ticks
//!         |
//!         v
//! +-----------+    Market snapshot     +------------+
//! | VenueFeed |----------------------->| Reconciler |<--- StaleSweep
//! +-----------+                        +------------+     (self, state timeout)
//!       |                                   ^
//!       |      feed lifecycle (watch)       |
//!       +-----------------------------------+
//!
//! orders
//! ------
//!    Submit / Cancel / ReconcileAll
//!                 |
//!                 v
//!         +-------------+   Ack (reconcile resolves unknown)
//!         | OrderRouter |------------------------------------+
//!         +-------------+                                    |
//!                 |  call: Place / Cancel / Query            |
//!                 v                                          v
//!         +--------------+   Ack / Fill / Cancelled     +--------+
//!         | VenueGateway |----------------------------->| Ledger |
//!         +--------------+                              +--------+
//!              |      ^
//!              +------+
//!            DeliverFill (self, delayed)
//!
//! safety
//! ------
//!    venue-subtree lifecycle
//!         | ordered lifecycle stream (cumulative restart counters)
//!         v
//!    event pump --RestartsObserved--> Health
//!                                       | KillSwitch (breaker trips)
//!                                       v
//!    gateways <---- CancelAll ------ Control ----> closes intake gate (Router)
//! ```
//!
//! 1. Market data: injected `FeedMsg::Tick`s flow through `VenueFeed` to the
//!    reconciler, which marks the venue Fresh and re-arms its stale sweep.
//!    Feed crashes reach the reconciler as watch events instead.
//! 2. Orders: `RouterMsg::Submit` records a pending intent and spawns a
//!    deadline-bounded `Place` call to a gateway, which acks to the ledger
//!    and schedules a delayed fill that reports `LedgerMsg::Fill` if the
//!    order was not cancelled in the interim. The context-owned offload answers the
//!    submitter and sends `SubmitResolved` back to the router; a timed-out
//!    call marks the intent unknown until `ReconcileAll` resolves it
//!    against the exchange.
//! 3. Safety: venue transitions flow from the supervisor's lifecycle stream
//!    through a mailbox pump into `Health`; cumulative event counters drive
//!    the breaker, which
//!    trips `Control`'s kill switch:
//!    intake closes and open orders are cancelled.
//!
//! Telemetry sits across all of it: actors stamp `enqueued_at` on messages
//! and record queue and handler latencies, and the sampler logs supervisor
//! snapshots, with one final WARN-level dump at exit.
//!
//! `main` runs phases 0–8 as an acceptance script: readiness-gated startup,
//! deterministic order flow, staleness, crash/restart, saturation and
//! conflation, the breaker tripping, and final telemetry evidence.

mod control;
mod health;
mod ledger;
mod messages;
mod reconciler;
mod router;
mod telemetry;
mod venue;

use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    ActorNode, ActorSlot, CancellationToken, DownReason, MailboxMode, Supervision,
    observe::{ChildLifecycleWatch, LifecycleWatchGuard},
    prelude::*,
};
use metrics_util::debugging::Snapshotter;
use tokio::time::Instant;

use control::Control;
use health::Health;
use ledger::Ledger;
use messages::*;
use reconciler::Reconciler;
use router::OrderRouter;
use telemetry::LatencyRecorder;
use venue::{ExchangeSim, VenueFeedFactory, VenueGatewayFactory};

const VENUE_A: VenueId = "venue-a";
const VENUE_B: VenueId = "venue-b";
const INIT_TIMEOUT: Duration = Duration::from_secs(2);
const PHASE_TIMEOUT: Duration = Duration::from_secs(3);
const URGENT_BOUND: Duration = Duration::from_secs(2);

type AnyError = Box<dyn Error + Send + Sync>;

fn take_node(nodes: &mut HashMap<String, ActorNode>, label: &str) -> ActorNode {
    nodes
        .remove(label)
        .unwrap_or_else(|| panic!("graph contains `{label}`"))
}

fn feed_message_key(message: &FeedMsg) -> &'static str {
    match message {
        FeedMsg::Tick(snapshot) => snapshot.symbol,
        // Control traffic must have a dedicated key so a later market-data
        // snapshot cannot conflate away a pending crash command.
        FeedMsg::Crash => "__control__",
    }
}

/// Venue actors keep the shallower mailbox the separate `trading-venues` graph
/// gave them before the scopes merged into one graph.
const VENUE_MAILBOX: usize = 16;

fn venue_slot<M: Send + 'static>(label: &str) -> ActorSlot<M> {
    ActorSlot::new(label).mailbox_capacity(VENUE_MAILBOX)
}

fn feed_slot(label: &str) -> ActorSlot<FeedMsg> {
    venue_slot(label)
        .mailbox(MailboxMode::conflate_by_key(feed_message_key))
        .message_size(messages::feed_message_size)
}

/// The core cyclic wiring declaration. Supervision topology stays explicit.
#[derive(Supervision)]
struct TradingEngine {
    reconciler: Reconciler,
    ledger: Ledger,
    #[supervision(label = "order-router")]
    router: OrderRouter,
    control: Control,
    health: Health,
}

struct App {
    runtime: kokage::Runtime,
    venue_a_feed: ActorRef<FeedMsg>,
    venue_b_feed: ActorRef<FeedMsg>,
    router: ActorRef<RouterMsg>,
    ledger: ActorRef<LedgerMsg>,
    reconciler: ActorRef<ReconcilerMsg>,
    control: ActorRef<ControlMsg>,
    health: ActorRef<HealthMsg>,
    venue_a: ExchangeSim,
    venue_b: ExchangeSim,
    intake_gate: Arc<AtomicBool>,
    background_stop: CancellationToken,
    sampler: tokio::task::JoinHandle<()>,
    lifecycle_watch: LifecycleWatchGuard,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    // Keep routine runtime INFO events compact; the sampler emits one final
    // WARN-level snapshot so tracing evidence remains visible.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;
    let metrics = telemetry::install_metrics()?;
    let latency = LatencyRecorder::default();
    let app = build_app(latency.clone()).await?;

    phase_0(&app).await?;
    phase_1(&app).await?;
    phase_2(&app).await?;
    phase_3(&app).await?;
    phase_4(&app).await?;
    phase_5(&app).await?;
    phase_6(&app).await?;
    phase_7(&app).await?;
    phase_8(app, latency, metrics).await?;
    Ok(())
}

async fn build_app(latency: LatencyRecorder) -> Result<App, AnyError> {
    let venue_a = ExchangeSim::default();
    let venue_b = ExchangeSim::default();
    let intake_gate = Arc::new(AtomicBool::new(true));

    // Open the configured venue slots first. The derived core wiring can then
    // capture their refs, and the venue factories can be defined afterward
    // with the core refs, closing the feed↔reconciler cycle.
    let mut builder = kokage::GraphBuilder::new();
    builder.name("trading-engine").mailbox_capacity(32);
    let venue_a_feed_slot = feed_slot("venues.venue-a-feed");
    let venue_a_feed = venue_a_feed_slot.actor_ref();
    let venue_a_gateway_slot = venue_slot("venues.venue-a-gateway");
    let venue_a_gateway = venue_a_gateway_slot.actor_ref();
    let venue_b_feed_slot = feed_slot("venues.venue-b-feed");
    let venue_b_feed = venue_b_feed_slot.actor_ref();
    let venue_b_gateway_slot = venue_slot("venues.venue-b-gateway");
    let venue_b_gateway = venue_b_gateway_slot.actor_ref();

    let refs = TradingEngine::wire(&mut builder, |refs| {
        let feed_refs = HashMap::from([
            (VENUE_A, venue_a_feed.clone()),
            (VENUE_B, venue_b_feed.clone()),
        ]);
        let gateways = HashMap::from([
            (VENUE_A, venue_a_gateway.clone()),
            (VENUE_B, venue_b_gateway.clone()),
        ]);
        TradingEngineFactories {
            reconciler: {
                let exchanges = vec![(VENUE_A, venue_a.clone()), (VENUE_B, venue_b.clone())];
                move || Reconciler::new(feed_refs.clone(), exchanges.clone())
            },
            ledger: {
                let latency = latency.clone();
                move || Ledger::new(latency.clone())
            },
            router: {
                let ledger = refs.ledger.clone();
                let intake_gate = intake_gate.clone();
                // Order keys correlate the router's pipelined gateway calls, so
                // the sequence lives outside the per-incarnation state: keys
                // must stay unique across router restarts (see router.rs).
                let sequence = Arc::new(AtomicU64::new(0));
                move || {
                    OrderRouter::new(
                        gateways.clone(),
                        ledger.clone(),
                        intake_gate.clone(),
                        sequence.clone(),
                    )
                }
            },
            control: {
                let intake_gate = intake_gate.clone();
                let venue_a_gateway = venue_a_gateway.clone();
                let venue_b_gateway = venue_b_gateway.clone();
                move || Control {
                    gateways: vec![venue_a_gateway.clone(), venue_b_gateway.clone()],
                    intake_gate: intake_gate.clone(),
                }
            },
            health: {
                let control = refs.control.clone();
                move || Health::new(control.clone())
            },
        }
    });

    let TradingEngineRefs {
        reconciler,
        ledger,
        router,
        control,
        health,
    } = refs;

    builder.define(
        venue_a_feed_slot,
        VenueFeedFactory {
            venue: VENUE_A,
            exchange: venue_a.clone(),
            reconciler: reconciler.clone(),
            latency: latency.clone(),
        },
    );
    builder.define(
        venue_a_gateway_slot,
        VenueGatewayFactory {
            venue: VENUE_A,
            exchange: venue_a.clone(),
            ledger: ledger.clone(),
            latency: latency.clone(),
        },
    );
    builder.define(
        venue_b_feed_slot,
        VenueFeedFactory {
            venue: VENUE_B,
            exchange: venue_b.clone(),
            reconciler: reconciler.clone(),
            latency: latency.clone(),
        },
    );
    builder.define(
        venue_b_gateway_slot,
        VenueGatewayFactory {
            venue: VENUE_B,
            exchange: venue_b.clone(),
            ledger: ledger.clone(),
            latency: latency.clone(),
        },
    );

    let graph = builder.build()?;
    let mut nodes: HashMap<_, _> = graph
        .into_nodes()
        .into_iter()
        .map(|node| (node.label().to_owned(), node))
        .collect();
    let venues = OrderedTree::new()
        .restart_config(RestartConfig::new(5, Duration::from_secs(10)))
        .actor(take_node(&mut nodes, "venues.venue-a-feed").child_id("venue-a-feed"))
        .actor(take_node(&mut nodes, "venues.venue-a-gateway").child_id("venue-a-gateway"))
        .actor(take_node(&mut nodes, "venues.venue-b-feed").child_id("venue-b-feed"))
        .actor(take_node(&mut nodes, "venues.venue-b-gateway").child_id("venue-b-gateway"));
    let tree = OrderedTree::new()
        .subtree("venues", venues)
        .actor(take_node(&mut nodes, "reconciler"))
        .actor(take_node(&mut nodes, "ledger"))
        .actor(take_node(&mut nodes, "order-router"))
        .actor(take_node(&mut nodes, "control"))
        .actor(take_node(&mut nodes, "health"));
    assert!(nodes.is_empty(), "every graph actor is placed exactly once");

    let runtime = tree.spawn()?;

    let background_stop = CancellationToken::new();
    let sampler = tokio::spawn(telemetry::sample(runtime.handle(), background_stop.clone()));
    // The aggregate restart breaker is fed from the lifecycle stream's
    // cumulative counters rather than inferring restarts from event pairs.
    // Every event carries the venues supervisor's cumulative restart counter;
    // even a `Lagged` gap loses transition detail but not the next count. The
    // scope is direct children, so nested per-venue supervisors would each
    // need their own lifecycle pump.
    let lifecycle_watch = runtime
        .subtree("venues")
        .expect("venues runtime subtree")
        .watch_lifecycle_to(&health, |event| HealthMsg::RestartsObserved {
            total: event.total_restarts,
        });

    Ok(App {
        runtime,
        venue_a_feed,
        venue_b_feed,
        router,
        ledger,
        reconciler,
        control,
        health,
        venue_a,
        venue_b,
        intake_gate,
        background_stop,
        sampler,
        lifecycle_watch,
    })
}

async fn phase_0(app: &App) -> Result<(), AnyError> {
    tokio::time::timeout(INIT_TIMEOUT, app.runtime.wait_started()).await??;
    assert_eq!(app.venue_a.feed_sessions(VENUE_A), 1);
    assert_eq!(app.venue_b.feed_sessions(VENUE_B), 1);
    assert_eq!(app.venue_a.gateway_sessions(VENUE_A), 1);
    assert_eq!(app.venue_b.gateway_sessions(VENUE_B), 1);
    println!("PHASE 0 OK — readiness-gated startup");
    Ok(())
}

async fn phase_1(app: &App) -> Result<(OrderKey, OrderKey), AnyError> {
    tick(&app.venue_a_feed, VENUE_A, "BTC-USD", 1).await?;
    tick(&app.venue_b_feed, VENUE_B, "BTC-USD", 1).await?;
    await_until(|| async {
        status(&app.reconciler)
            .await
            .is_some_and(|status| both_health(&status, VenueHealth::Fresh))
    })
    .await?;

    let order_1 = expect_placed(submit(&app.router, "OK", 2, VENUE_A).await?)?;
    await_until(|| async {
        ledger_report(&app.ledger).await.is_some_and(|report| {
            report
                .effects
                .get(&order_1)
                .is_some_and(|effects| effects.acknowledgements == 1 && effects.fills == 1)
        })
    })
    .await?;

    let order_2 = expect_placed(submit(&app.router, "OK", 1, VENUE_B).await?)?;
    let cancelled = bounded_call(&app.router, |reply| RouterMsg::Cancel {
        key: order_2.clone(),
        reply,
    })
    .await?;
    assert_eq!(cancelled, CancelOutcome::Cancelled);
    await_until(|| async {
        ledger_report(&app.ledger).await.is_some_and(|report| {
            report
                .effects
                .get(&order_2)
                .is_some_and(|effects| effects.cancellations == 1)
        })
    })
    .await?;
    println!("PHASE 1 OK — deterministic market and order flow");
    Ok((order_1, order_2))
}

async fn phase_2(app: &App) -> Result<(), AnyError> {
    tick(&app.venue_b_feed, VENUE_B, "BTC-USD", 2).await?;
    await_until(|| async {
        status(&app.reconciler)
            .await
            .is_some_and(|status| status.venues.get(VENUE_B) == Some(&VenueHealth::Fresh))
    })
    .await?;
    let b_transition_count = status(&app.reconciler)
        .await
        .expect("reconciler available")
        .transitions[VENUE_B]
        .len();
    let keepalive_stop = CancellationToken::new();
    let keepalive = tokio::spawn({
        let feed = app.venue_b_feed.clone();
        let stop = keepalive_stop.clone();
        async move {
            let mut seq = 10;
            while !stop.is_cancelled() {
                let _ = tick(&feed, VENUE_B, "BTC-USD", seq).await;
                seq += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
    let before_b = generation(app, "venue-b-feed");
    let venues = app
        .runtime
        .subtree("venues")
        .expect("venues runtime subtree");
    let (lifecycle, baseline) = restart_observer(&venues, "venue-a-feed");
    app.venue_a_feed.send(FeedMsg::Crash).await?;
    tokio::time::timeout(
        PHASE_TIMEOUT,
        await_restart(lifecycle, "venue-a-feed", baseline),
    )
    .await?;
    await_until(|| async {
        status(&app.reconciler).await.is_some_and(|status| {
            status
                .transitions
                .get(VENUE_A)
                .is_some_and(|log| log.contains(&VenueHealth::Down))
        })
    })
    .await?;
    tick(&app.venue_a_feed, VENUE_A, "BTC-USD", 2).await?;
    await_until(|| async {
        status(&app.reconciler).await.is_some_and(|status| {
            status.venues.get(VENUE_A) == Some(&VenueHealth::Fresh)
                && status.venues.get(VENUE_B) == Some(&VenueHealth::Fresh)
        })
    })
    .await?;
    keepalive_stop.cancel();
    keepalive.await?;
    let final_status = status(&app.reconciler).await.expect("reconciler available");
    assert_eq!(generation(app, "venue-b-feed"), before_b);
    assert_eq!(
        final_status.transitions[VENUE_B].len(),
        b_transition_count,
        "venue-b must stay Fresh throughout venue-a recovery"
    );
    assert!(!final_status.transitions[VENUE_B].contains(&VenueHealth::Down));
    assert!(
        final_status.down_reasons[VENUE_A].contains(&DownReason::Failure),
        "venue-a monitor must report the scripted panic as Failure"
    );
    assert!(!bounded_call(&app.health, |reply| HealthMsg::Tripped { reply }).await?);
    println!("PHASE 2 OK — isolated panic, Down, and recovery");
    Ok(())
}

async fn phase_3(app: &App) -> Result<(), AnyError> {
    tick(&app.venue_b_feed, VENUE_B, "BTC-USD", 3).await?;
    let keepalive_stop = CancellationToken::new();
    let keepalive = tokio::spawn({
        let feed = app.venue_a_feed.clone();
        let stop = keepalive_stop.clone();
        async move {
            let mut seq = 100;
            while !stop.is_cancelled() {
                let _ = tick(&feed, VENUE_A, "BTC-USD", seq).await;
                seq += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
    await_until(|| async {
        status(&app.reconciler).await.is_some_and(|status| {
            status.venues.get(VENUE_A) == Some(&VenueHealth::Fresh)
                && status.venues.get(VENUE_B) == Some(&VenueHealth::Stale)
        })
    })
    .await?;
    keepalive_stop.cancel();
    keepalive.await?;
    tick(&app.venue_b_feed, VENUE_B, "BTC-USD", 4).await?;
    await_until(|| async {
        status(&app.reconciler)
            .await
            .is_some_and(|status| status.venues.get(VENUE_B) == Some(&VenueHealth::Fresh))
    })
    .await?;
    println!("PHASE 3 OK — independent state-timeout staleness");
    Ok(())
}

async fn phase_4(app: &App) -> Result<OrderKey, AnyError> {
    let started = Instant::now();
    let stalled = tokio::spawn({
        let router = app.router.clone();
        async move { submit(&router, "STALL-NOACCEPT", 3, VENUE_A).await }
    });
    // The router pipelines gateway calls off its handle loop (see router.rs),
    // so a venue-a Place that stalls for the full CALL_DEADLINE must not
    // head-of-line block unrelated traffic. Prove it while the stall is in
    // flight: wait for the router to register the pending intent, then place
    // an order on healthy venue-b and require both to finish inside the
    // stalled call's deadline.
    await_until(|| async {
        bounded_call(&app.router, |reply| RouterMsg::InFlight { reply })
            .await
            .is_ok_and(|count| count >= 1)
    })
    .await?;
    let healthy = expect_placed(submit(&app.router, "OK", 1, VENUE_B).await?)?;
    assert!(
        started.elapsed() < CALL_DEADLINE,
        "healthy-venue order must not queue behind the stalled venue call"
    );
    let key = expect_unknown(stalled.await??)?;
    assert!(started.elapsed() < CALL_DEADLINE * 4);
    await_until(|| async {
        ledger_report(&app.ledger).await.is_some_and(|report| {
            report
                .effects
                .get(&healthy)
                .is_some_and(|effects| effects.acknowledgements == 1)
        })
    })
    .await?;
    await_until(|| async {
        bounded_call(&app.router, |reply| RouterMsg::ReconcileAll { reply })
            .await
            .is_ok_and(|resolved| resolved == 1)
    })
    .await?;
    assert_eq!(app.venue_a.accept_count(&key), 1);
    await_until(|| async { app.venue_a.status(&key) == Some(OrderStatus::Filled) }).await?;
    println!("PHASE 4 OK — stalled call pipelined, healthy venue live, reconciled");
    Ok(key)
}

async fn phase_5(app: &App) -> Result<OrderKey, AnyError> {
    let key = expect_unknown(submit(&app.router, "ACCEPT-NOACK", 4, VENUE_B).await?)?;
    await_until(|| async {
        bounded_call(&app.router, |reply| RouterMsg::ReconcileAll { reply })
            .await
            .is_ok_and(|resolved| resolved == 1)
    })
    .await?;
    assert_eq!(app.venue_b.accept_count(&key), 1);
    let report = ledger_report(&app.ledger).await.expect("ledger available");
    assert_eq!(
        report
            .effects
            .get(&key)
            .expect("reconciled ledger effect")
            .acknowledgements,
        1
    );
    println!("PHASE 5 OK — unknown accepted order reconciled once");
    Ok(key)
}

async fn phase_6(app: &App) -> Result<(), AnyError> {
    let open = expect_placed(submit(&app.router, "OPEN", 5, VENUE_A).await?)?;
    let flood_stop = CancellationToken::new();
    let flood = tokio::spawn({
        let a = app.venue_a_feed.clone();
        let b = app.venue_b_feed.clone();
        let stop = flood_stop.clone();
        async move {
            let mut seq = 1_000;
            while !stop.is_cancelled() {
                let now = Instant::now();
                let _ = a
                    .send(FeedMsg::Tick(snapshot(VENUE_A, "BTC-USD", seq, now)))
                    .await;
                let _ = b
                    .send(FeedMsg::Tick(snapshot(VENUE_B, "ETH-USD", seq, now)))
                    .await;
                seq += 1;
            }
        }
    });
    await_until(|| async {
        let stats = app
            .runtime
            .subtree("venues")
            .expect("venue runtime subtree")
            .actor_stats();
        ["venues.venue-a-feed", "venues.venue-b-feed"]
            .iter()
            .all(|id| {
                stats
                    .iter()
                    .find(|sample| sample.actor_id == *id)
                    .is_some_and(|sample| sample.messages_conflated > 0)
            })
    })
    .await?;
    let cancelled = app
        .control
        .call(URGENT_BOUND, |reply| ControlMsg::EmergencyCancelAll {
            reply,
        })
        .await?;
    assert!(cancelled >= 1);
    assert_eq!(app.venue_a.status(&open), Some(OrderStatus::Cancelled));
    flood_stop.cancel();
    flood.await?;
    let feed_stats = app
        .runtime
        .subtree("venues")
        .expect("venue runtime subtree")
        .actor_stats();
    for id in ["venues.venue-a-feed", "venues.venue-b-feed"] {
        assert!(
            feed_stats
                .iter()
                .find(|stats| stats.actor_id == id)
                .is_some_and(|stats| stats.messages_conflated > 0),
            "{id} must demonstrate latest-wins conflation"
        );
    }
    println!("PHASE 6 OK — urgent control stayed responsive under saturation");
    Ok(())
}

async fn phase_7(app: &App) -> Result<(), AnyError> {
    app.health.send(HealthMsg::ResetBreaker).await?;
    let venues = app
        .runtime
        .subtree("venues")
        .expect("venues runtime subtree");
    for (id, feed) in [
        ("venue-a-feed", &app.venue_a_feed),
        ("venue-b-feed", &app.venue_b_feed),
        ("venue-a-feed", &app.venue_a_feed),
        ("venue-b-feed", &app.venue_b_feed),
    ] {
        let (lifecycle, baseline) = restart_observer(&venues, id);
        feed.send(FeedMsg::Crash).await?;
        tokio::time::timeout(PHASE_TIMEOUT, await_restart(lifecycle, id, baseline)).await?;
    }
    await_until(|| async {
        bounded_call(&app.health, |reply| HealthMsg::Tripped { reply })
            .await
            .unwrap_or(false)
    })
    .await?;
    assert_eq!(
        submit(&app.router, "OK", 1, VENUE_A).await?,
        SubmitResult::IntakeClosed
    );
    println!("PHASE 7 OK — aggregate venue restart breaker tripped");
    Ok(())
}

async fn phase_8(app: App, latency: LatencyRecorder, metrics: Snapshotter) -> Result<(), AnyError> {
    assert!(!app.intake_gate.load(Ordering::Acquire));
    let _ = bounded_call(&app.router, |reply| RouterMsg::ReconcileAll { reply }).await?;
    await_until(|| async {
        bounded_call(&app.router, |reply| RouterMsg::InFlight { reply })
            .await
            .is_ok_and(|count| count == 0)
    })
    .await?;
    let final_ledger = ledger_report(&app.ledger).await.expect("ledger available");
    assert!(!final_ledger.effects.is_empty());
    assert!(
        final_ledger
            .effects
            .values()
            .all(|effects| { effects.fills >= 1 || effects.cancellations >= 1 }),
        "every ledger key must be terminal before supervisor shutdown"
    );

    // Sibling shutdown is concurrent. The application stages teardown first:
    // intake is closed, unknown intent is reconciled, and only then are venue
    // and support actors stopped. DrainPolicy alone does not order siblings.
    app.background_stop.cancel();
    app.sampler.await?;
    drop(app.lifecycle_watch);
    tokio::time::timeout(Duration::from_secs(5), app.runtime.shutdown_and_wait()).await??;

    let latency = latency.snapshot();
    for series in [
        "queue.market",
        "handler.feed",
        "queue.fill",
        "handler.gateway",
    ] {
        assert!(latency.get(series).is_some_and(|sample| sample.count > 0));
    }
    println!("latency summary: {latency:#?}");
    let selected_metrics = metrics
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            matches!(
                key.key().name(),
                "actor.message.bytes_accepted" | "supervisor.restarts"
            )
        })
        .collect::<Vec<_>>();
    assert!(
        selected_metrics
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == "actor.message.bytes_accepted")
            .count()
            >= 2
    );
    println!("selected metrics: {selected_metrics:#?}");
    println!("final supervisor snapshot: {:#?}", app.runtime.snapshot());
    println!("final actor stats: {:#?}", app.runtime.actor_stats());
    println!("PHASE 8 OK — staged shutdown and observability");
    Ok(())
}

async fn bounded_call<M, T>(
    actor: &ActorRef<M>,
    message: impl FnOnce(Reply<T>) -> M,
) -> Result<T, AnyError>
where
    M: Send + 'static,
    T: Send + 'static,
{
    Ok(actor.call(PHASE_TIMEOUT, message).await?)
}

fn restart_observer(handle: &RuntimeHandle, id: &str) -> (ChildLifecycleWatch, u64) {
    let lifecycle = handle.watch_lifecycle();
    let generation = handle
        .snapshot()
        .child(id)
        .unwrap_or_else(|| panic!("{id} is supervised"))
        .generation;
    (lifecycle, generation)
}

async fn await_restart(mut lifecycle: ChildLifecycleWatch, id: &str, baseline: u64) {
    lifecycle
        .started_after(id, baseline)
        .await
        .unwrap_or_else(|| panic!("lifecycle closed before {id} restarted"));
}

async fn await_until<F, Fut>(mut predicate: F) -> Result<(), AnyError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(PHASE_TIMEOUT, async {
        loop {
            if predicate().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;
    Ok(())
}

async fn submit(
    router: &ActorRef<RouterMsg>,
    symbol: &'static str,
    qty: i64,
    venue: VenueId,
) -> Result<SubmitResult, AnyError> {
    bounded_call(router, |reply| RouterMsg::Submit {
        symbol,
        qty,
        venue,
        reply,
    })
    .await
}

async fn status(reconciler: &ActorRef<ReconcilerMsg>) -> Option<ReconcilerStatus> {
    bounded_call(reconciler, |reply| ReconcilerMsg::Status { reply })
        .await
        .ok()
}

async fn ledger_report(ledger: &ActorRef<LedgerMsg>) -> Option<LedgerReport> {
    bounded_call(ledger, |reply| LedgerMsg::Report { reply })
        .await
        .ok()
}

async fn tick(
    feed: &ActorRef<FeedMsg>,
    venue: VenueId,
    symbol: &'static str,
    seq: u64,
) -> Result<(), AnyError> {
    tokio::time::timeout(
        PHASE_TIMEOUT,
        feed.send(FeedMsg::Tick(snapshot(venue, symbol, seq, Instant::now()))),
    )
    .await??;
    Ok(())
}

fn snapshot(
    venue: VenueId,
    symbol: &'static str,
    seq: u64,
    enqueued_at: Instant,
) -> MarketSnapshot {
    MarketSnapshot {
        venue,
        symbol,
        bid: 100_000 + seq as i64,
        ask: 100_010 + seq as i64,
        seq,
        enqueued_at,
    }
}

fn both_health(status: &ReconcilerStatus, health: VenueHealth) -> bool {
    [VENUE_A, VENUE_B]
        .iter()
        .all(|venue| status.venues.get(venue) == Some(&health))
}

fn generation(app: &App, child: &str) -> u64 {
    app.runtime
        .snapshot()
        .descendant(["venues", child])
        .expect("nested child snapshot")
        .generation
}

fn expect_placed(result: SubmitResult) -> Result<OrderKey, AnyError> {
    match result {
        SubmitResult::Placed(key) => Ok(key),
        other => Err(format!("expected placed order, got {other:?}").into()),
    }
}

fn expect_unknown(result: SubmitResult) -> Result<OrderKey, AnyError> {
    match result {
        SubmitResult::Unknown(key) => Ok(key),
        other => Err(format!("expected unknown order, got {other:?}").into()),
    }
}
