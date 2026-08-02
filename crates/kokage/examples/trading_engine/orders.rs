use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use kokage::prelude::*;
use tokio::time::Instant;

use crate::{
    protocol::{
        CancelOutcome, GATEWAY_DEADLINE, GatewayCallResult, GatewayMsg, LedgerMsg, LedgerReport,
        OFFLOAD_DEADLINE, OrderKey, PlaceOutcome, PlacePurpose, QueryOutcome, ReconcileReport,
        RouterMsg, RouterReport, SubmitResult, Symbol, VenueId,
    },
    telemetry::LatencyRecorder,
};

#[derive(kokage::ActorFactory)]
pub struct Ledger {
    pub latency: LatencyRecorder,
    #[factory(default)]
    report: LedgerReport,
}

impl Actor for Ledger {
    type Msg = LedgerMsg;

    async fn handle(&mut self, message: LedgerMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            LedgerMsg::Acknowledged { key, venue } => {
                tracing::debug!(venue, order_key = key, "order acknowledged");
                self.report.effects.entry(key).or_default().acknowledgements += 1;
            }
            LedgerMsg::Filled {
                key,
                venue,
                quantity,
                enqueued_at,
            } => {
                tracing::debug!(venue, quantity, order_key = key, "order filled");
                self.latency.record(
                    "fill.queue",
                    Instant::now().saturating_duration_since(enqueued_at),
                );
                self.report.effects.entry(key).or_default().fills += 1;
            }
            LedgerMsg::Cancelled { key, venue } => {
                tracing::debug!(venue, order_key = key, "order cancelled");
                self.report.effects.entry(key).or_default().cancellations += 1;
            }
            LedgerMsg::Report { reply } => reply.send(self.report.clone()),
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntentState {
    Pending,
    Unknown,
    Confirmed,
    Cancelled,
}

#[derive(Debug)]
struct OrderIntent {
    venue: VenueId,
    symbol: Symbol,
    quantity: i64,
    state: IntentState,
    cancel_requested: bool,
    reconcile_place_in_flight: bool,
    cancel_in_flight: bool,
    cancel_replies: Vec<Reply<CancelOutcome>>,
}

struct Sweep {
    id: u64,
    examined: usize,
    resolved: usize,
    remaining: HashSet<OrderKey>,
    reply: Reply<ReconcileReport>,
}

#[derive(kokage::ActorFactory)]
pub struct OrderRouter {
    pub gateways: HashMap<VenueId, ActorRef<GatewayMsg>>,
    pub ledger: ActorRef<LedgerMsg>,
    pub intake_gate: Arc<AtomicBool>,
    pub sequence: Arc<AtomicU64>,
    #[factory(default)]
    intents: HashMap<OrderKey, OrderIntent>,
    #[factory(default)]
    next_sweep: u64,
    #[factory(default)]
    sweep: Option<Sweep>,
}

impl OrderRouter {
    fn launch_place(
        &self,
        key: OrderKey,
        venue: VenueId,
        symbol: Symbol,
        quantity: i64,
        purpose: PlacePurpose,
        ctx: &mut Context<'_, Self>,
    ) {
        let gateway = self.gateways.get(venue).expect("known venue").clone();
        let completion_key = key.clone();
        ctx.offload(
            OFFLOAD_DEADLINE,
            async move {
                gateway
                    .call(
                        |reply| GatewayMsg::Place {
                            key,
                            symbol,
                            quantity,
                            reply,
                        },
                        GATEWAY_DEADLINE,
                    )
                    .await
                    .map(|PlaceOutcome { .. }| GatewayCallResult::Acknowledged)
                    .unwrap_or(GatewayCallResult::Unknown)
            },
            move |result| RouterMsg::PlaceCompleted {
                key: completion_key,
                purpose,
                result: result.unwrap_or(GatewayCallResult::Unknown),
            },
        );
    }

    fn launch_query(&self, sweep: u64, key: OrderKey, venue: VenueId, ctx: &mut Context<'_, Self>) {
        let gateway = self.gateways.get(venue).expect("known venue").clone();
        let completion_key = key.clone();
        ctx.offload(
            OFFLOAD_DEADLINE,
            async move {
                gateway
                    .call(|reply| GatewayMsg::Query { key, reply }, GATEWAY_DEADLINE)
                    .await
                    .ok()
            },
            move |result| RouterMsg::QueryCompleted {
                sweep,
                key: completion_key,
                result: result.unwrap_or(None),
            },
        );
    }

    fn launch_cancel(&self, key: OrderKey, venue: VenueId, ctx: &mut Context<'_, Self>) {
        let gateway = self.gateways.get(venue).expect("known venue").clone();
        let completion_key = key.clone();
        ctx.offload(
            OFFLOAD_DEADLINE,
            async move {
                gateway
                    .call(|reply| GatewayMsg::Cancel { key, reply }, GATEWAY_DEADLINE)
                    .await
                    .unwrap_or(CancelOutcome::Unknown)
            },
            move |result| RouterMsg::CancelCompleted {
                key: completion_key,
                result: result.unwrap_or(CancelOutcome::Unknown),
            },
        );
    }

    fn finish_sweep_key(&mut self, sweep_id: u64, key: &str, resolved: bool) {
        let Some(sweep) = self.sweep.as_mut().filter(|sweep| sweep.id == sweep_id) else {
            return;
        };
        if !sweep.remaining.remove(key) {
            return;
        }
        if resolved {
            sweep.resolved += 1;
        }
        if sweep.remaining.is_empty() {
            let sweep = self.sweep.take().expect("active sweep exists");
            sweep.reply.send(ReconcileReport {
                examined: sweep.examined,
                resolved: sweep.resolved,
                busy: false,
            });
        }
    }

    fn report(&self) -> RouterReport {
        let mut report = RouterReport {
            reconciling: self.sweep.is_some(),
            ..RouterReport::default()
        };
        for intent in self.intents.values() {
            match intent.state {
                IntentState::Pending => report.pending += 1,
                IntentState::Unknown => report.unknown += 1,
                IntentState::Confirmed => report.confirmed += 1,
                IntentState::Cancelled => report.cancelled += 1,
            }
        }
        report
    }
}

// Every venue call starts here and finishes as another RouterMsg. The handler
// never awaits place, cancel, query, or reconciliation re-place calls, so the
// router remains a responsive serialization point for its own state.
impl Actor for OrderRouter {
    type Msg = RouterMsg;

    async fn handle(&mut self, message: RouterMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            RouterMsg::Submit {
                venue,
                symbol,
                quantity,
                reply,
            } => {
                if !self.intake_gate.load(Ordering::Acquire) {
                    reply.send(SubmitResult::IntakeClosed);
                    return Ok(());
                }
                if !self.gateways.contains_key(venue) {
                    reply.send(SubmitResult::UnknownVenue);
                    return Ok(());
                }
                let key = format!(
                    "order-{}",
                    self.sequence.fetch_add(1, Ordering::Relaxed) + 1
                );
                self.intents.insert(
                    key.clone(),
                    OrderIntent {
                        venue,
                        symbol,
                        quantity,
                        state: IntentState::Pending,
                        cancel_requested: false,
                        reconcile_place_in_flight: false,
                        cancel_in_flight: false,
                        cancel_replies: Vec::new(),
                    },
                );
                self.launch_place(
                    key,
                    venue,
                    symbol,
                    quantity,
                    PlacePurpose::Submit(reply),
                    ctx,
                );
            }
            RouterMsg::PlaceCompleted {
                key,
                purpose,
                result,
            } => match purpose {
                PlacePurpose::Submit(reply) => {
                    let submitted = match result {
                        GatewayCallResult::Acknowledged => {
                            if let Some(intent) = self.intents.get_mut(&key) {
                                intent.state = IntentState::Confirmed;
                            }
                            SubmitResult::Placed(key)
                        }
                        GatewayCallResult::Unknown => {
                            if let Some(intent) = self.intents.get_mut(&key) {
                                intent.state = IntentState::Unknown;
                            }
                            SubmitResult::Unknown(key)
                        }
                    };
                    // State changes before the client is released, preserving
                    // ordering for an immediate follow-up request.
                    reply.send(submitted);
                }
                PlacePurpose::Reconcile { sweep } => {
                    let Some(intent) = self.intents.get_mut(&key) else {
                        self.finish_sweep_key(sweep, &key, false);
                        return Ok(());
                    };
                    intent.reconcile_place_in_flight = false;
                    let cancel_requested = intent.cancel_requested;
                    let venue = intent.venue;
                    let launch_cancel = cancel_requested && !intent.cancel_in_flight;
                    if launch_cancel {
                        intent.cancel_in_flight = true;
                    }

                    if cancel_requested {
                        // A cancel that arrived after Query(NotFound) waits
                        // for this re-place call to finish, then follows it at
                        // the gateway. Its completion must never resurrect the
                        // intent in router state.
                        self.finish_sweep_key(sweep, &key, false);
                        if launch_cancel {
                            self.launch_cancel(key, venue, ctx);
                        }
                    } else if result == GatewayCallResult::Acknowledged {
                        if let Some(intent) = self.intents.get_mut(&key) {
                            intent.state = IntentState::Confirmed;
                        }
                        self.finish_sweep_key(sweep, &key, true);
                    } else {
                        self.finish_sweep_key(sweep, &key, false);
                    }
                }
            },
            RouterMsg::Cancel { key, reply } => {
                let Some(intent) = self.intents.get_mut(&key) else {
                    reply.send(CancelOutcome::NotFound);
                    return Ok(());
                };
                intent.cancel_requested = true;
                intent.cancel_replies.push(reply);
                let venue = intent.venue;
                let launch_cancel = !intent.reconcile_place_in_flight && !intent.cancel_in_flight;
                if launch_cancel {
                    intent.cancel_in_flight = true;
                }

                // This key no longer belongs to the sweep: a later query
                // completion is stale, and an in-flight re-place completion
                // will launch the causally-following cancel above.
                if let Some(sweep) = self
                    .sweep
                    .as_ref()
                    .filter(|sweep| sweep.remaining.contains(&key))
                    .map(|sweep| sweep.id)
                {
                    self.finish_sweep_key(sweep, &key, false);
                }
                if launch_cancel {
                    self.launch_cancel(key, venue, ctx);
                }
            }
            RouterMsg::CancelCompleted { key, result } => {
                if let Some(intent) = self.intents.get_mut(&key) {
                    intent.cancel_in_flight = false;
                    if result == CancelOutcome::Cancelled
                        || (result == CancelOutcome::NotFound
                            && intent.state == IntentState::Unknown)
                    {
                        intent.state = IntentState::Cancelled;
                    }
                    for reply in std::mem::take(&mut intent.cancel_replies) {
                        reply.send(result);
                    }
                }
            }
            RouterMsg::ReconcileAll { reply } => {
                if self.sweep.is_some() {
                    reply.send(ReconcileReport {
                        busy: true,
                        ..ReconcileReport::default()
                    });
                    return Ok(());
                }
                let unknown = self
                    .intents
                    .iter()
                    .filter(|(_, intent)| {
                        intent.state == IntentState::Unknown && !intent.cancel_requested
                    })
                    .map(|(key, intent)| (key.clone(), intent.venue))
                    .collect::<Vec<_>>();
                if unknown.is_empty() {
                    reply.send(ReconcileReport::default());
                    return Ok(());
                }
                self.next_sweep += 1;
                let sweep = self.next_sweep;
                self.sweep = Some(Sweep {
                    id: sweep,
                    examined: unknown.len(),
                    resolved: 0,
                    remaining: unknown.iter().map(|(key, _)| key.clone()).collect(),
                    reply,
                });
                for (key, venue) in unknown {
                    self.launch_query(sweep, key, venue, ctx);
                }
            }
            RouterMsg::QueryCompleted { sweep, key, result } => {
                if !self
                    .sweep
                    .as_ref()
                    .is_some_and(|active| active.id == sweep && active.remaining.contains(&key))
                {
                    return Ok(());
                }
                if self
                    .intents
                    .get(&key)
                    .is_some_and(|intent| intent.cancel_requested)
                {
                    self.finish_sweep_key(sweep, &key, false);
                    return Ok(());
                }
                match result {
                    Some(QueryOutcome::Found(_)) => {
                        let intent = self.intents.get_mut(&key).expect("swept intent exists");
                        intent.state = IntentState::Confirmed;
                        self.ledger
                            .send(LedgerMsg::Acknowledged {
                                key: key.clone(),
                                venue: intent.venue,
                            })
                            .await?;
                        self.finish_sweep_key(sweep, &key, true);
                    }
                    Some(QueryOutcome::NotFound) => {
                        let intent = self.intents.get_mut(&key).expect("swept intent exists");
                        intent.reconcile_place_in_flight = true;
                        let venue = intent.venue;
                        let symbol = intent.symbol;
                        let quantity = intent.quantity;
                        self.launch_place(
                            key,
                            venue,
                            symbol,
                            quantity,
                            PlacePurpose::Reconcile { sweep },
                            ctx,
                        );
                    }
                    None => self.finish_sweep_key(sweep, &key, false),
                }
            }
            RouterMsg::Report { reply } => reply.send(self.report()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kokage::ActorSlot;
    use tokio::sync::Notify;

    use super::*;
    use crate::{
        protocol::{PHASE_TIMEOUT, STALL_FOREVER},
        telemetry::LatencyRecorder,
    };

    #[derive(Debug, Default)]
    struct GatewayState {
        place_attempts: usize,
        cancel_attempts: usize,
        live: bool,
    }

    #[derive(Clone, Debug, Default)]
    struct GatewayScript {
        state: Arc<Mutex<GatewayState>>,
        release_reconcile_place: Arc<Notify>,
    }

    impl GatewayScript {
        fn inspect<T>(&self, inspect: impl FnOnce(&GatewayState) -> T) -> T {
            inspect(&self.state.lock().expect("gateway script lock poisoned"))
        }
    }

    #[derive(kokage::ActorFactory)]
    struct RacingGateway {
        script: GatewayScript,
        #[factory(default)]
        stalled_replies: Vec<Reply<PlaceOutcome>>,
    }

    impl Actor for RacingGateway {
        type Msg = GatewayMsg;

        async fn handle(
            &mut self,
            message: GatewayMsg,
            _ctx: &mut Context<'_, Self>,
        ) -> ExitResult {
            match message {
                GatewayMsg::Place { key, reply, .. } => {
                    let attempt = {
                        let mut state = self
                            .script
                            .state
                            .lock()
                            .expect("gateway script lock poisoned");
                        state.place_attempts += 1;
                        if state.place_attempts > 1 {
                            state.live = true;
                        }
                        state.place_attempts
                    };
                    if attempt == 1 {
                        self.stalled_replies.push(reply);
                    } else {
                        let release = self.script.release_reconcile_place.clone();
                        tokio::spawn(async move {
                            release.notified().await;
                            reply.send(PlaceOutcome { key });
                        });
                    }
                }
                GatewayMsg::Cancel { reply, .. } => {
                    let outcome = {
                        let mut state = self
                            .script
                            .state
                            .lock()
                            .expect("gateway script lock poisoned");
                        state.cancel_attempts += 1;
                        if state.live {
                            state.live = false;
                            CancelOutcome::Cancelled
                        } else {
                            CancelOutcome::NotFound
                        }
                    };
                    reply.send(outcome);
                }
                GatewayMsg::Query { reply, .. } => reply.send(QueryOutcome::NotFound),
                GatewayMsg::CancelAll { reply } => reply.send(0),
                GatewayMsg::DeliverFills => {}
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn cancel_waits_for_an_in_flight_reconcile_place_and_tombstones_the_intent() {
        let script = GatewayScript::default();
        let gateway_slot = ActorSlot::<GatewayMsg>::new("racing-gateway");
        let gateway = gateway_slot.actor_ref();
        let ledger_slot = ActorSlot::<LedgerMsg>::new("ledger");
        let ledger = ledger_slot.actor_ref();
        let router_slot = ActorSlot::<RouterMsg>::new("router");
        let router = router_slot.actor_ref();

        let gateway_spec = gateway_slot.define(RacingGatewayFactory {
            script: script.clone(),
        });
        let ledger_spec = ledger_slot.define(LedgerFactory {
            latency: LatencyRecorder::default(),
        });
        let router_spec = router_slot.define(OrderRouterFactory {
            gateways: HashMap::from([("venue", gateway)]),
            ledger,
            intake_gate: Arc::new(AtomicBool::new(true)),
            sequence: Arc::new(AtomicU64::new(0)),
        });
        let mut tree = Tree::new();
        tree.add_actor_spec(gateway_spec);
        tree.add_actor_spec(ledger_spec);
        tree.add_actor_spec(router_spec);
        let running = tree.spawn().expect("test tree starts");
        tokio::time::timeout(PHASE_TIMEOUT, running.scope().wait_started())
            .await
            .expect("test tree readiness is bounded")
            .expect("test tree becomes ready");

        let key = match crate::submit(&router, "venue", STALL_FOREVER, 1)
            .await
            .expect("stalled submit completes as unknown")
        {
            SubmitResult::Unknown(key) => key,
            result => panic!("expected unknown submit, got {result:?}"),
        };

        let reconcile = tokio::spawn({
            let router = router.clone();
            async move { crate::bounded_call(&router, |reply| RouterMsg::ReconcileAll { reply }).await }
        });
        crate::await_until(|| async { script.inspect(|state| state.place_attempts == 2) })
            .await
            .expect("reconciliation reaches its held re-place");
        assert!(script.inspect(|state| state.live));

        let cancel = tokio::spawn({
            let router = router.clone();
            let key = key.clone();
            async move { crate::bounded_call(&router, |reply| RouterMsg::Cancel { key, reply }).await }
        });
        assert_eq!(
            reconcile
                .await
                .expect("reconcile task joins")
                .expect("cancel releases the sweep"),
            ReconcileReport {
                examined: 1,
                resolved: 0,
                busy: false,
            }
        );
        assert_eq!(script.inspect(|state| state.cancel_attempts), 0);

        script.release_reconcile_place.notify_one();
        assert_eq!(
            cancel
                .await
                .expect("cancel task joins")
                .expect("cancel completes after the held place"),
            CancelOutcome::Cancelled
        );
        assert!(script.inspect(|state| !state.live));
        assert_eq!(script.inspect(|state| state.cancel_attempts), 1);

        let report = crate::router_report(&router)
            .await
            .expect("router report succeeds");
        assert_eq!(
            (report.unknown, report.confirmed, report.cancelled),
            (0, 0, 1)
        );
        assert_eq!(
            crate::bounded_call(&router, |reply| RouterMsg::ReconcileAll { reply })
                .await
                .expect("tombstoned intent is not swept again"),
            ReconcileReport::default()
        );

        tokio::time::timeout(PHASE_TIMEOUT, running.shutdown())
            .await
            .expect("test shutdown is bounded")
            .expect("test tree shuts down cleanly");
    }
}
