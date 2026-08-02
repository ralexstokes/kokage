//! A simulated multi-venue trading engine that serves as its own acceptance
//! test. There is no socket or external service: each venue is represented by
//! a shared [`ExchangeSim`](venue::ExchangeSim), and `main` drives numbered,
//! assertion-heavy phases through the same actor APIs a real edge would use.
//!
//! ```text
//! root
//! ├── venues (restart-budgeted OneForOne)
//! │   ├── venue-a-feed ───────┐
//! │   ├── venue-a-gateway ─┐  │
//! │   ├── venue-b-feed ────┼──┼─────┐
//! │   └── venue-b-gateway ─┘  │     │
//! ├── ledger ◀────────────────┘     │ order effects
//! ├── market-reconciler ◀───────────┘ ticks + feed watches
//! ├── order-router ───────────────────── pipelined gateway calls
//! ├── control ────────────────────────── atomic gate + cancel-all fanout
//! └── health-breaker ─────────────────── cumulative venue restart stream
//! ```
//!
//! Every ref is minted from an [`ActorSlot`] before any factory is defined.
//! That makes the feed/reconciler and gateway/core dependency cycles explicit
//! without a registry or late `Option<ActorRef<_>>` wiring.
//!
//! Phases 2 and 6 deliberately panic feed actors. Their panic messages and
//! the resulting WARN-level restart traces are expected acceptance evidence.

mod market;
mod orders;
mod protocol;
mod safety;
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
    ActorSlot, CancellationToken, Guard, Mailbox, RestartPolicy, ScopeRef,
    observe::SupervisorSnapshotReceiver, prelude::*,
};
use metrics_util::debugging::Snapshotter;
use tokio::time::Instant;

use market::{MarketReconcilerFactory, STALE_AFTER};
use orders::{LedgerFactory, OrderRouterFactory};
use protocol::*;
use safety::{BREAKER_THRESHOLD, ControlFactory, HealthBreakerFactory};
use telemetry::{LatencyRecorder, SamplerEvidence};
use venue::{ExchangeSim, VenueFeedFactory, VenueGatewayFactory};

const INIT_TIMEOUT: Duration = Duration::from_secs(2);
const HEALTHY_VENUE_BOUND: Duration = Duration::from_millis(250);
const URGENT_CONTROL_BOUND: Duration = Duration::from_millis(300);
const VENUE_MAILBOX: usize = 16;
const VENUE_RESTART_LIMIT: usize = 4;

// Phase 2 crashes venue A once. Phase 6 then alternates at most
// BREAKER_THRESHOLD crashes across both feeds. Keep the per-child budget
// strictly above that worst-case count so the breaker, not restart intensity,
// owns the safety decision.
const _: () = assert!(VENUE_RESTART_LIMIT > 1 + BREAKER_THRESHOLD.div_ceil(2));

type AnyError = Box<dyn Error + Send + Sync>;

struct App {
    running: kokage::RunningTree,
    venues: ScopeRef,
    feed_a: ActorRef<FeedMsg>,
    feed_b: ActorRef<FeedMsg>,
    router: ActorRef<RouterMsg>,
    ledger: ActorRef<LedgerMsg>,
    reconciler: ActorRef<ReconcilerMsg>,
    control: ActorRef<ControlMsg>,
    health: ActorRef<HealthMsg>,
    exchange_a: ExchangeSim,
    exchange_b: ExchangeSim,
    intake_gate: Arc<AtomicBool>,
    latency: LatencyRecorder,
    sampler_evidence: SamplerEvidence,
    sampler_stop: CancellationToken,
    sampler: tokio::task::JoinHandle<()>,
    lifecycle_pump: Guard,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;
    let metrics = telemetry::install_metrics()?;
    let app = build_app()?;

    phase_0_readiness(&app).await?;
    phase_1_deterministic_flow(&app).await?;
    phase_2_feed_isolation(&app).await?;
    phase_3_independent_staleness(&app).await?;
    phase_4_pipelining_and_reconciliation(&app).await?;
    phase_5_control_under_flood(&app).await?;
    phase_6_restart_breaker(&app).await?;
    phase_7_shutdown_and_telemetry(app, metrics).await?;
    Ok(())
}

fn build_app() -> Result<App, AnyError> {
    let exchange_a = ExchangeSim::default();
    let exchange_b = ExchangeSim::default();
    let latency = LatencyRecorder::default();
    let intake_gate = Arc::new(AtomicBool::new(true));

    // Refs first. Factories below can now capture dependencies in any order.
    let feed_a_slot = ActorSlot::<FeedMsg>::new("venue-a-feed");
    let feed_a = feed_a_slot.actor_ref();
    let gateway_a_slot = ActorSlot::<GatewayMsg>::new("venue-a-gateway");
    let gateway_a = gateway_a_slot.actor_ref();
    let feed_b_slot = ActorSlot::<FeedMsg>::new("venue-b-feed");
    let feed_b = feed_b_slot.actor_ref();
    let gateway_b_slot = ActorSlot::<GatewayMsg>::new("venue-b-gateway");
    let gateway_b = gateway_b_slot.actor_ref();

    let router_slot = ActorSlot::<RouterMsg>::new("order-router");
    let router = router_slot.actor_ref();
    let ledger_slot = ActorSlot::<LedgerMsg>::new("ledger");
    let ledger = ledger_slot.actor_ref();
    let reconciler_slot = ActorSlot::<ReconcilerMsg>::new("market-reconciler");
    let reconciler = reconciler_slot.actor_ref();
    let control_slot = ActorSlot::<ControlMsg>::new("control");
    let control = control_slot.actor_ref();
    let health_slot = ActorSlot::<HealthMsg>::new("health-breaker");
    let health = health_slot.actor_ref();

    let ledger_spec = ledger_slot.define(LedgerFactory {
        latency: latency.clone(),
    });
    let reconciler_spec = reconciler_slot.define(MarketReconcilerFactory {
        feeds: HashMap::from([(VENUE_A, feed_a.clone()), (VENUE_B, feed_b.clone())]),
        exchanges: vec![(VENUE_A, exchange_a.clone()), (VENUE_B, exchange_b.clone())],
    });
    let router_spec = router_slot.define(OrderRouterFactory {
        gateways: HashMap::from([(VENUE_A, gateway_a.clone()), (VENUE_B, gateway_b.clone())]),
        ledger: ledger.clone(),
        intake_gate: intake_gate.clone(),
        sequence: Arc::new(AtomicU64::new(0)),
    });
    let control_spec = control_slot.define(ControlFactory {
        gateways: vec![gateway_a.clone(), gateway_b.clone()],
        intake_gate: intake_gate.clone(),
    });
    let health_spec = health_slot.define(HealthBreakerFactory {
        control: control.clone(),
    });

    let feed_spec = |slot: ActorSlot<FeedMsg>, factory| {
        slot.define(factory)
            .mailbox(Mailbox::latest_by_key(VENUE_MAILBOX, feed_message_key))
            .message_size(feed_message_size)
    };
    let feed_a_spec = feed_spec(
        feed_a_slot,
        VenueFeedFactory {
            venue: VENUE_A,
            exchange: exchange_a.clone(),
            reconciler: reconciler.clone(),
            latency: latency.clone(),
        },
    );
    let feed_b_spec = feed_spec(
        feed_b_slot,
        VenueFeedFactory {
            venue: VENUE_B,
            exchange: exchange_b.clone(),
            reconciler: reconciler.clone(),
            latency: latency.clone(),
        },
    );
    let gateway_a_spec = gateway_a_slot
        .define(VenueGatewayFactory {
            venue: VENUE_A,
            exchange: exchange_a.clone(),
            ledger: ledger.clone(),
            latency: latency.clone(),
        })
        .mailbox(Mailbox::queue(VENUE_MAILBOX));
    let gateway_b_spec = gateway_b_slot
        .define(VenueGatewayFactory {
            venue: VENUE_B,
            exchange: exchange_b.clone(),
            ledger: ledger.clone(),
            latency: latency.clone(),
        })
        .mailbox(Mailbox::queue(VENUE_MAILBOX));

    let mut venue_tree = Tree::new().default_child_restart(
        RestartPolicy::on_failure().limit(VENUE_RESTART_LIMIT, Duration::from_secs(30)),
    );
    venue_tree.add_actor_spec(feed_a_spec);
    venue_tree.add_actor_spec(gateway_a_spec);
    venue_tree.add_actor_spec(feed_b_spec);
    venue_tree.add_actor_spec(gateway_b_spec);

    let mut tree = Tree::new().default_actor_mailbox_capacity(64);
    tree.add_subtree("venues", venue_tree);
    tree.add_actor_spec(ledger_spec);
    tree.add_actor_spec(reconciler_spec);
    tree.add_actor_spec(router_spec);
    tree.add_actor_spec(control_spec);
    tree.add_actor_spec(health_spec);

    let running = tree.spawn()?;
    let venues = running
        .scope()
        .subtree("venues")
        .expect("venues subtree is present");

    // Feed the safety actor with idempotent cumulative restart counts from
    // the direct-child lifecycle stream.
    let mut restart_total = venues.snapshot().total_restarts;
    let lifecycle_pump =
        venues
            .subscribe_lifecycle()
            .direct_children()
            .forward_to(&health, move |event| {
                if let Some(total) = event.total_restarts() {
                    restart_total = total;
                }
                HealthMsg::RestartsObserved {
                    total: restart_total,
                }
            });

    let sampler_evidence = SamplerEvidence::default();
    let sampler_stop = CancellationToken::new();
    let sampler = tokio::spawn(telemetry::sample_snapshots(
        running.scope(),
        sampler_stop.clone(),
        sampler_evidence.clone(),
    ));

    Ok(App {
        running,
        venues,
        feed_a,
        feed_b,
        router,
        ledger,
        reconciler,
        control,
        health,
        exchange_a,
        exchange_b,
        intake_gate,
        latency,
        sampler_evidence,
        sampler_stop,
        sampler,
        lifecycle_pump,
    })
}

async fn phase_0_readiness(app: &App) -> Result<(), AnyError> {
    tokio::time::timeout(INIT_TIMEOUT, app.running.scope().wait_started()).await??;
    assert_eq!(app.exchange_a.feed_sessions(), 1);
    assert_eq!(app.exchange_a.gateway_sessions(), 1);
    assert_eq!(app.exchange_b.feed_sessions(), 1);
    assert_eq!(app.exchange_b.gateway_sessions(), 1);
    assert!(app.intake_gate.load(Ordering::Acquire));
    assert!(!health_report(&app.health).await?.tripped);
    println!("PHASE 0 OK — readiness-gated venue startup");
    Ok(())
}

async fn phase_1_deterministic_flow(app: &App) -> Result<(), AnyError> {
    tick(&app.feed_a, VENUE_A, "BTC-USD", 101).await?;
    tick(&app.feed_b, VENUE_B, "BTC-USD", 202).await?;
    await_until(|| async {
        market_report(&app.reconciler).await.is_ok_and(|report| {
            both_venues(&report, VenueCondition::Fresh)
                && report.sequences.get(VENUE_A) == Some(&Some(101))
                && report.sequences.get(VENUE_B) == Some(&Some(202))
        })
    })
    .await?;

    let filled = expect_placed(submit(&app.router, VENUE_A, NORMAL_ORDER, 2).await?)?;
    await_effects(&app.ledger, &filled, 1, 1, 0).await?;
    assert_eq!(app.exchange_a.status(&filled), Some(OrderStatus::Filled));

    let open = expect_placed(submit(&app.router, VENUE_B, KEEP_OPEN, 3).await?)?;
    let cancelled = bounded_call(&app.router, |reply| RouterMsg::Cancel {
        key: open.clone(),
        reply,
    })
    .await?;
    assert_eq!(cancelled, CancelOutcome::Cancelled);
    await_effects(&app.ledger, &open, 1, 0, 1).await?;
    assert_eq!(app.exchange_b.status(&open), Some(OrderStatus::Cancelled));
    println!("PHASE 1 OK — deterministic market, fill, and cancel flow");
    Ok(())
}

async fn phase_2_feed_isolation(app: &App) -> Result<(), AnyError> {
    let a_sessions = app.exchange_a.feed_sessions();
    let b_generation = child_generation(&app.venues, "venue-b-feed");
    let (snapshots, baseline) = restart_observer(&app.venues, "venue-a-feed");
    app.feed_a.send(FeedMsg::Crash).await?;
    tokio::time::timeout(
        PHASE_TIMEOUT,
        await_restart(snapshots, "venue-a-feed", baseline),
    )
    .await??;
    await_until(|| async {
        market_report(&app.reconciler).await.is_ok_and(|report| {
            report
                .transitions
                .get(VENUE_A)
                .is_some_and(|transitions| transitions.contains(&VenueCondition::Down))
                && report
                    .exits
                    .get(VENUE_A)
                    .is_some_and(|exits| exits.iter().any(kokage::observe::ExitStatus::is_failure))
        })
    })
    .await?;

    tick(&app.feed_a, VENUE_A, "BTC-USD", 303).await?;
    await_until(|| async {
        market_report(&app.reconciler).await.is_ok_and(|report| {
            report.conditions.get(VENUE_A) == Some(&VenueCondition::Fresh)
                && report.sequences.get(VENUE_A) == Some(&Some(303))
        })
    })
    .await?;
    assert_eq!(app.exchange_a.feed_sessions(), a_sessions + 1);
    assert_eq!(child_generation(&app.venues, "venue-b-feed"), b_generation);
    let report = market_report(&app.reconciler).await?;
    assert!(!report.transitions[VENUE_B].contains(&VenueCondition::Down));
    println!("PHASE 2 OK — one feed panic isolated, observed Down, and recovered");
    Ok(())
}

async fn phase_3_independent_staleness(app: &App) -> Result<(), AnyError> {
    tick(&app.feed_a, VENUE_A, "BTC-USD", 400).await?;
    tick(&app.feed_b, VENUE_B, "BTC-USD", 400).await?;
    await_until(|| async {
        market_report(&app.reconciler)
            .await
            .is_ok_and(|report| both_venues(&report, VenueCondition::Fresh))
    })
    .await?;

    let stop = CancellationToken::new();
    let keepalive = tokio::spawn({
        let feed = app.feed_a.clone();
        let stop = stop.clone();
        async move {
            let mut sequence = 401;
            let mut interval = tokio::time::interval(STALE_AFTER / 4);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = interval.tick() => {
                        let _ = tick(&feed, VENUE_A, "BTC-USD", sequence).await;
                        sequence += 1;
                    }
                }
            }
        }
    });
    await_until(|| async {
        market_report(&app.reconciler).await.is_ok_and(|report| {
            report.conditions.get(VENUE_A) == Some(&VenueCondition::Fresh)
                && report.conditions.get(VENUE_B) == Some(&VenueCondition::Stale)
        })
    })
    .await?;
    stop.cancel();
    keepalive.await?;

    tick(&app.feed_b, VENUE_B, "BTC-USD", 401).await?;
    await_until(|| async {
        market_report(&app.reconciler)
            .await
            .is_ok_and(|report| report.conditions.get(VENUE_B) == Some(&VenueCondition::Fresh))
    })
    .await?;
    println!("PHASE 3 OK — venue-local staleness deadlines are independent");
    Ok(())
}

async fn phase_4_pipelining_and_reconciliation(app: &App) -> Result<(), AnyError> {
    let stalled = tokio::spawn({
        let router = app.router.clone();
        async move { submit(&router, VENUE_A, STALL_FOREVER, 4).await }
    });
    await_until(|| async {
        router_report(&app.router)
            .await
            .is_ok_and(|report| report.pending == 1)
    })
    .await?;

    let healthy = tokio::time::timeout(
        HEALTHY_VENUE_BOUND,
        submit(&app.router, VENUE_B, NORMAL_ORDER, 1),
    )
    .await??;
    let healthy = expect_placed(healthy)?;
    await_effects(&app.ledger, &healthy, 1, 1, 0).await?;

    let no_ack = expect_unknown(submit(&app.router, VENUE_B, ACCEPT_NO_ACK, 5).await?)?;
    let stalled = expect_unknown(stalled.await??)?;
    assert_eq!(app.exchange_a.accept_count(&stalled), 0);
    assert_eq!(app.exchange_b.accept_count(&no_ack), 1);

    let reconciled = bounded_call(&app.router, |reply| RouterMsg::ReconcileAll { reply }).await?;
    assert_eq!(
        reconciled,
        ReconcileReport {
            examined: 2,
            resolved: 2,
            busy: false,
        }
    );
    await_effects(&app.ledger, &stalled, 1, 1, 0).await?;
    await_effects(&app.ledger, &no_ack, 1, 0, 0).await?;
    assert_eq!(app.exchange_a.place_attempts(&stalled), 2);
    assert_eq!(app.exchange_a.accept_count(&stalled), 1);
    assert_eq!(app.exchange_b.place_attempts(&no_ack), 1);
    assert_eq!(app.exchange_b.accept_count(&no_ack), 1);

    // The accepted-but-unacknowledged order had no fill scheduled. Cancel it
    // through the same non-blocking router path so every effect is terminal.
    assert_eq!(
        bounded_call(&app.router, |reply| RouterMsg::Cancel {
            key: no_ack.clone(),
            reply,
        })
        .await?,
        CancelOutcome::Cancelled
    );
    await_effects(&app.ledger, &no_ack, 1, 0, 1).await?;
    assert_eq!(
        bounded_call(&app.router, |reply| RouterMsg::ReconcileAll { reply }).await?,
        ReconcileReport::default(),
        "a second sweep must not repeat a resolved effect"
    );
    let router = router_report(&app.router).await?;
    assert_eq!((router.pending, router.unknown), (0, 0));
    println!("PHASE 4 OK — stalled venue bypassed; unknowns reconciled exactly once");
    Ok(())
}

async fn phase_5_control_under_flood(app: &App) -> Result<(), AnyError> {
    let open = expect_placed(submit(&app.router, VENUE_A, KEEP_OPEN, 6).await?)?;
    let stop = CancellationToken::new();
    let flood = tokio::spawn({
        let feed_a = app.feed_a.clone();
        let feed_b = app.feed_b.clone();
        let stop = stop.clone();
        async move {
            let mut sequence = 10_000;
            while !stop.is_cancelled() {
                let now = Instant::now();
                let _ = feed_a.try_send(FeedMsg::Tick(snapshot(VENUE_A, "BTC-USD", sequence, now)));
                let _ = feed_b.try_send(FeedMsg::Tick(snapshot(VENUE_B, "ETH-USD", sequence, now)));
                sequence += 1;
                if sequence % 64 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
    });
    await_until(|| async {
        ["venue-a-feed", "venue-b-feed"].iter().all(|id| {
            app.venues
                .actor_stats()
                .iter()
                .find(|sample| sample.stats.actor_id == *id)
                .is_some_and(|sample| sample.stats.messages_conflated > 0)
        })
    })
    .await?;

    let cancelled = tokio::time::timeout(
        URGENT_CONTROL_BOUND,
        bounded_call(&app.control, |reply| ControlMsg::EmergencyCancelAll {
            reply,
        }),
    )
    .await??;
    assert!(cancelled >= 1);
    assert_eq!(app.exchange_a.status(&open), Some(OrderStatus::Cancelled));
    assert!(app.intake_gate.load(Ordering::Acquire));
    stop.cancel();
    flood.await?;

    for id in ["venue-a-feed", "venue-b-feed"] {
        let stats = app
            .venues
            .actor_stats()
            .into_iter()
            .find(|sample| sample.stats.actor_id == id)
            .unwrap_or_else(|| panic!("stats for {id}"));
        assert!(stats.stats.messages_conflated > 0);
        assert!(stats.stats.messages_received < stats.stats.messages_accepted);
    }
    println!("PHASE 5 OK — urgent cancel-all stayed responsive during feed conflation");
    Ok(())
}

async fn phase_6_restart_breaker(app: &App) -> Result<(), AnyError> {
    let open = expect_placed(submit(&app.router, VENUE_B, KEEP_OPEN, 7).await?)?;
    let feeds = [("venue-a-feed", &app.feed_a), ("venue-b-feed", &app.feed_b)];
    let mut crashes = 0;
    while !health_report(&app.health).await?.tripped && crashes < BREAKER_THRESHOLD {
        let (id, feed) = feeds[crashes % feeds.len()];
        let (snapshots, baseline) = restart_observer(&app.venues, id);
        feed.send(FeedMsg::Crash).await?;
        tokio::time::timeout(PHASE_TIMEOUT, await_restart(snapshots, id, baseline)).await??;
        crashes += 1;
    }
    await_until(|| async {
        let restart_total = app.venues.snapshot().total_restarts;
        health_report(&app.health)
            .await
            .is_ok_and(|report| report.tripped && report.observed_total == restart_total)
            && !app.intake_gate.load(Ordering::Acquire)
            && app.exchange_b.status(&open) == Some(OrderStatus::Cancelled)
    })
    .await?;
    let health = health_report(&app.health).await?;
    assert!(health.restarts_in_window >= BREAKER_THRESHOLD);
    assert_eq!(health.observed_total, app.venues.snapshot().total_restarts);
    assert_eq!(
        submit(&app.router, VENUE_A, NORMAL_ORDER, 1).await?,
        SubmitResult::IntakeClosed
    );
    println!("PHASE 6 OK — sliding restart breaker closed intake and cancelled open orders");
    Ok(())
}

async fn phase_7_shutdown_and_telemetry(app: App, metrics: Snapshotter) -> Result<(), AnyError> {
    assert!(!app.intake_gate.load(Ordering::Acquire));
    let reconciled = bounded_call(&app.router, |reply| RouterMsg::ReconcileAll { reply }).await?;
    assert!(!reconciled.busy);
    await_until(|| async {
        router_report(&app.router)
            .await
            .is_ok_and(|report| report.pending == 0 && report.unknown == 0 && !report.reconciling)
    })
    .await?;
    let _ = bounded_call(&app.control, |reply| ControlMsg::EmergencyCancelAll {
        reply,
    })
    .await?;
    await_until(|| async {
        ledger_report(&app.ledger).await.is_ok_and(|report| {
            !report.effects.is_empty()
                && report
                    .effects
                    .values()
                    .all(|effect| effect.fills == 1 || effect.cancellations == 1)
        })
    })
    .await?;

    // Sibling shutdown has no ordering guarantee. Finish application work,
    // stop out-of-tree telemetry, then stop the supervision tree.
    app.sampler_stop.cancel();
    app.sampler.await?;
    let sampler = app.sampler_evidence.report();
    assert!(sampler.samples > 0 && sampler.actors_seen >= 9);
    drop(app.lifecycle_pump);
    let final_snapshot = app.running.scope().snapshot();
    let final_stats = app.running.scope().actor_stats();
    tokio::time::timeout(Duration::from_secs(5), app.running.shutdown()).await??;

    let latency = app.latency.snapshot();
    for name in ["feed.queue", "feed.handle", "gateway.handle", "fill.queue"] {
        assert!(latency.get(name).is_some_and(|series| series.count > 0));
    }
    let metric_snapshot = metrics.snapshot().into_vec();
    for name in ["actor.message.bytes_accepted", "supervisor.restarts"] {
        assert!(
            metric_snapshot
                .iter()
                .any(|(key, _, _, _)| key.key().name() == name),
            "missing expected metric {name}"
        );
    }
    let selected_metrics = metric_snapshot
        .into_iter()
        .filter(|(key, _, _, _)| {
            matches!(
                key.key().name(),
                "actor.message.bytes_accepted" | "supervisor.restarts"
            )
        })
        .collect::<Vec<_>>();
    println!(
        "telemetry evidence: latency_series={}, metrics={}, samples={}, actors={}, root_children={}, actor_stats={}",
        latency.len(),
        selected_metrics.len(),
        sampler.samples,
        sampler.actors_seen,
        final_snapshot.children.len(),
        final_stats.len(),
    );
    println!("PHASE 7 OK — staged shutdown preserved final telemetry evidence");
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
    Ok(actor.call(message, PHASE_TIMEOUT).await?)
}

async fn await_until<F, Fut>(mut predicate: F) -> Result<(), AnyError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(PHASE_TIMEOUT, async {
        loop {
            if predicate().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

async fn submit(
    router: &ActorRef<RouterMsg>,
    venue: VenueId,
    symbol: Symbol,
    quantity: i64,
) -> Result<SubmitResult, AnyError> {
    bounded_call(router, |reply| RouterMsg::Submit {
        venue,
        symbol,
        quantity,
        reply,
    })
    .await
}

async fn market_report(actor: &ActorRef<ReconcilerMsg>) -> Result<MarketReport, AnyError> {
    bounded_call(actor, |reply| ReconcilerMsg::Report { reply }).await
}

async fn router_report(actor: &ActorRef<RouterMsg>) -> Result<RouterReport, AnyError> {
    bounded_call(actor, |reply| RouterMsg::Report { reply }).await
}

async fn health_report(actor: &ActorRef<HealthMsg>) -> Result<HealthReport, AnyError> {
    bounded_call(actor, |reply| HealthMsg::Report { reply }).await
}

async fn ledger_report(actor: &ActorRef<LedgerMsg>) -> Result<LedgerReport, AnyError> {
    bounded_call(actor, |reply| LedgerMsg::Report { reply }).await
}

async fn await_effects(
    ledger: &ActorRef<LedgerMsg>,
    key: &str,
    acknowledgements: usize,
    fills: usize,
    cancellations: usize,
) -> Result<(), AnyError> {
    await_until(|| async {
        ledger_report(ledger).await.is_ok_and(|report| {
            report.effects.get(key).is_some_and(|effects| {
                effects.acknowledgements == acknowledgements
                    && effects.fills == fills
                    && effects.cancellations == cancellations
            })
        })
    })
    .await
}

async fn tick(
    feed: &ActorRef<FeedMsg>,
    venue: VenueId,
    symbol: Symbol,
    sequence: u64,
) -> Result<(), AnyError> {
    feed.send(FeedMsg::Tick(snapshot(
        venue,
        symbol,
        sequence,
        Instant::now(),
    )))
    .await?;
    Ok(())
}

fn snapshot(venue: VenueId, symbol: Symbol, sequence: u64, enqueued_at: Instant) -> MarketTick {
    MarketTick {
        venue,
        symbol,
        bid: 100_000 + sequence as i64,
        ask: 100_010 + sequence as i64,
        sequence,
        enqueued_at,
    }
}

fn both_venues(report: &MarketReport, condition: VenueCondition) -> bool {
    [VENUE_A, VENUE_B]
        .iter()
        .all(|venue| report.conditions.get(venue) == Some(&condition))
}

fn restart_observer(scope: &ScopeRef, id: &str) -> (SupervisorSnapshotReceiver, u64) {
    let snapshots = scope.subscribe_snapshots();
    let generation = child_generation(scope, id);
    (snapshots, generation)
}

async fn await_restart(
    mut snapshots: SupervisorSnapshotReceiver,
    id: &str,
    generation: u64,
) -> Result<(), AnyError> {
    snapshots
        .wait_for_child(id, |child| {
            child.generation > generation && child.state.is_running()
        })
        .await
        .map_err(|_| format!("snapshot stream closed before {id} restarted"))?;
    Ok(())
}

fn child_generation(scope: &ScopeRef, id: &str) -> u64 {
    scope
        .snapshot()
        .child(id)
        .unwrap_or_else(|| panic!("{id} is supervised"))
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
