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

#[derive(Clone, Debug)]
struct OrderIntent {
    venue: VenueId,
    symbol: Symbol,
    quantity: i64,
    state: IntentState,
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
                    if result == GatewayCallResult::Acknowledged {
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
                let Some(intent) = self.intents.get(&key) else {
                    reply.send(CancelOutcome::NotFound);
                    return Ok(());
                };
                let gateway = self
                    .gateways
                    .get(intent.venue)
                    .expect("known venue")
                    .clone();
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
                        reply,
                    },
                );
            }
            RouterMsg::CancelCompleted { key, result, reply } => {
                if result == CancelOutcome::Cancelled
                    && let Some(intent) = self.intents.get_mut(&key)
                {
                    intent.state = IntentState::Cancelled;
                }
                reply.send(result);
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
                    .filter(|(_, intent)| intent.state == IntentState::Unknown)
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
                        let intent = self.intents.get(&key).expect("swept intent exists");
                        self.launch_place(
                            key,
                            intent.venue,
                            intent.symbol,
                            intent.quantity,
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
