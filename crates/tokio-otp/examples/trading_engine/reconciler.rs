use std::{collections::HashMap, time::Duration};
use tokio_otp::{LiveContext, StateTimeoutSlot};

use tokio::time::Instant;
use tokio_otp::prelude::*;

use crate::{
    messages::{FeedMsg, ReconcilerMsg, ReconcilerStatus, VenueHealth, VenueId},
    venue::ExchangeSim,
};

pub const STALE_AFTER: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
struct VenueState {
    last_seen: Option<Instant>,
    health: VenueHealth,
    transitions: Vec<VenueHealth>,
}

impl Default for VenueState {
    fn default() -> Self {
        Self {
            last_seen: None,
            health: VenueHealth::Stale,
            transitions: vec![VenueHealth::Stale],
        }
    }
}

pub struct Reconciler {
    feeds: HashMap<VenueId, ActorRef<FeedMsg>>,
    sessions: Vec<(VenueId, ExchangeSim)>,
    venues: HashMap<VenueId, VenueState>,
    down_reasons: HashMap<VenueId, Vec<DownReason>>,
    stale_sweep: StateTimeoutSlot,
}

impl Reconciler {
    pub fn new(
        feeds: HashMap<VenueId, ActorRef<FeedMsg>>,
        sessions: Vec<(VenueId, ExchangeSim)>,
    ) -> Self {
        let venues = feeds
            .keys()
            .copied()
            .map(|venue| (venue, VenueState::default()))
            .collect();
        Self {
            feeds,
            sessions,
            venues,
            down_reasons: HashMap::new(),
            stale_sweep: StateTimeoutSlot::new(),
        }
    }

    fn watch(&self, venue: VenueId, ctx: &impl LiveContext<ReconcilerMsg>) {
        let feed = self.feeds.get(venue).expect("known venue");
        ctx.watch(feed, move |event| ReconcilerMsg::Feed { venue, event });
    }

    fn transition(&mut self, venue: VenueId, health: VenueHealth) {
        let state = self.venues.get_mut(venue).expect("known venue");
        if state.health != health {
            state.health = health;
            state.transitions.push(health);
        }
    }

    fn rearm(&mut self, ctx: &impl LiveContext<ReconcilerMsg>) {
        let now = Instant::now();
        let earliest = self
            .venues
            .values()
            .filter(|state| state.health == VenueHealth::Fresh)
            .filter_map(|state| state.last_seen.map(|seen| seen + STALE_AFTER))
            .min();
        if let Some(deadline) = earliest {
            self.stale_sweep.set(ctx.send_after_retractable(
                ReconcilerMsg::StaleSweep,
                deadline.saturating_duration_since(now),
            ));
        } else {
            self.stale_sweep.clear();
        }
    }
}

impl Actor for Reconciler {
    type Msg = ReconcilerMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, ReconcilerMsg>) -> ActorResult {
        for (venue, exchange) in &self.sessions {
            assert!(
                exchange.feed_sessions(venue) >= 1,
                "nested venue readiness must complete before core startup"
            );
            assert!(
                exchange.gateway_sessions(venue) >= 1,
                "nested venue readiness must complete before core startup"
            );
        }
        for venue in self.feeds.keys().copied() {
            self.watch(venue, ctx);
        }
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: ReconcilerMsg,
        ctx: &mut MessageContext<'_, ReconcilerMsg>,
    ) -> ActorResult {
        match message {
            ReconcilerMsg::Market(snapshot) => {
                let venue = snapshot.venue;
                tracing::debug!(
                    venue,
                    symbol = snapshot.symbol,
                    sequence = snapshot.seq,
                    spread = snapshot.ask - snapshot.bid,
                    "market snapshot reconciled"
                );
                self.venues.get_mut(venue).expect("known venue").last_seen = Some(Instant::now());
                self.transition(venue, VenueHealth::Fresh);
                self.rearm(ctx);
            }
            ReconcilerMsg::Feed { venue, event } => {
                match event {
                    MonitorEvent::Up { generation, .. } => {
                        tracing::debug!(venue, generation, "venue feed up");
                        self.transition(venue, VenueHealth::Stale);
                    }
                    MonitorEvent::Down(down) => {
                        self.down_reasons
                            .entry(venue)
                            .or_default()
                            .push(down.reason);
                        self.transition(venue, VenueHealth::Down);
                    }
                    MonitorEvent::Terminated { .. } => {
                        self.transition(venue, VenueHealth::Down);
                    }
                    MonitorEvent::Lagged { dropped, .. } => {
                        // Overload resync point: the reconciler re-derives
                        // health from subsequent events and the next tick, so
                        // no transition is applied here.
                        tracing::debug!(venue, dropped, "venue feed monitor lagged");
                    }
                    _ => {}
                }
                self.rearm(ctx);
            }
            ReconcilerMsg::StaleSweep => {
                let now = Instant::now();
                let stale = self
                    .venues
                    .iter()
                    .filter_map(|(&venue, state)| {
                        (state.health == VenueHealth::Fresh
                            && state
                                .last_seen
                                .is_some_and(|seen| now.duration_since(seen) >= STALE_AFTER))
                        .then_some(venue)
                    })
                    .collect::<Vec<_>>();
                for venue in stale {
                    self.transition(venue, VenueHealth::Stale);
                }
                self.rearm(ctx);
            }
            ReconcilerMsg::Status { reply } => {
                reply.send(ReconcilerStatus {
                    venues: self
                        .venues
                        .iter()
                        .map(|(&venue, state)| (venue, state.health))
                        .collect(),
                    transitions: self
                        .venues
                        .iter()
                        .map(|(&venue, state)| (venue, state.transitions.clone()))
                        .collect(),
                    down_reasons: self.down_reasons.clone(),
                });
            }
        }
        Ok(Continue)
    }
}
