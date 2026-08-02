use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorRef, Context, ExitResult, StopContext,
    raw::{RawActor, RawContext},
};
use tokio::sync::{Mutex, Notify};

use crate::{
    common::{CALL_BOUND, Envelope, Evidence, EvidenceTx},
    journal::JournalMsg,
    router::RouterMsg,
};

#[derive(Default)]
struct TransportState {
    queue: VecDeque<Envelope>,
    acked: HashSet<u64>,
    deliveries: HashMap<u64, u64>,
    connected: bool,
    disconnect_before_ack: HashSet<u64>,
}

#[derive(Clone, Default)]
pub struct ChatTransport {
    state: Arc<Mutex<TransportState>>,
    changed: Arc<Notify>,
}

pub enum TransportEvent {
    Message(Envelope),
    Disconnected,
}

impl ChatTransport {
    pub async fn publish(&self, envelope: Envelope) {
        let mut state = self.state.lock().await;
        if state.acked.contains(&envelope.id) {
            return;
        }
        state.queue.push_back(envelope);
        drop(state);
        self.changed.notify_waiters();
    }

    pub async fn disconnect_before_ack(&self, envelope_id: u64) {
        self.state
            .lock()
            .await
            .disconnect_before_ack
            .insert(envelope_id);
    }

    pub async fn connect(&self) {
        self.state.lock().await.connected = true;
        self.changed.notify_waiters();
    }

    async fn next_event(&self) -> TransportEvent {
        loop {
            let notified = self.changed.notified();
            {
                let mut state = self.state.lock().await;
                if !state.connected {
                    return TransportEvent::Disconnected;
                }
                let envelope = state.queue.front().cloned();
                if let Some(envelope) = envelope {
                    *state.deliveries.entry(envelope.id).or_default() += 1;
                    if state.disconnect_before_ack.remove(&envelope.id) {
                        state.connected = false;
                    }
                    return TransportEvent::Message(envelope);
                }
            }
            notified.await;
        }
    }

    async fn ack(&self, envelope_id: u64) -> Result<(), ()> {
        let mut state = self.state.lock().await;
        if !state.connected {
            return Err(());
        }
        state.acked.insert(envelope_id);
        state.queue.retain(|envelope| envelope.id != envelope_id);
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn is_acked(&self, envelope_id: u64) -> bool {
        self.state.lock().await.acked.contains(&envelope_id)
    }

    pub async fn deliveries(&self, envelope_id: u64) -> u64 {
        self.state
            .lock()
            .await
            .deliveries
            .get(&envelope_id)
            .copied()
            .unwrap_or_default()
    }
}

#[derive(Clone, Default)]
pub struct ConnectGate {
    open: Arc<AtomicBool>,
    changed: Arc<Notify>,
}

impl ConnectGate {
    pub fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait(&self) {
        while !self.open.load(Ordering::Acquire) {
            let notified = self.changed.notified();
            if self.open.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }
}

pub enum OutboundMsg {
    Assistant { envelope_id: u64, text: String },
    Progress { envelope_id: u64, sequence: u64 },
}

pub struct OutboundSender {
    evidence: EvidenceTx,
}

impl OutboundSender {
    pub fn new(evidence: EvidenceTx) -> Self {
        Self { evidence }
    }
}

impl Actor for OutboundSender {
    type Msg = OutboundMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            OutboundMsg::Assistant { envelope_id, text } => {
                let _ = (envelope_id, text);
            }
            OutboundMsg::Progress {
                envelope_id,
                sequence,
            } => self.evidence.emit(Evidence::Progress {
                envelope_id,
                sequence,
            }),
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("outbound"));
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ProgressMsg {
    pub envelope_id: u64,
    pub sequence: u64,
}

#[derive(Clone, Default)]
pub struct ProgressGate {
    pub block: Arc<AtomicBool>,
    pub blocked: Arc<AtomicBool>,
    pub entered: Arc<Notify>,
    pub release: Arc<Notify>,
}

pub struct ProgressSender {
    outbound: ActorRef<OutboundMsg>,
    gate: ProgressGate,
    evidence: EvidenceTx,
}

impl ProgressSender {
    pub fn new(outbound: ActorRef<OutboundMsg>, gate: ProgressGate, evidence: EvidenceTx) -> Self {
        Self {
            outbound,
            gate,
            evidence,
        }
    }
}

impl Actor for ProgressSender {
    type Msg = ProgressMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.gate.block.load(Ordering::Acquire) {
            self.gate.blocked.store(true, Ordering::Release);
            self.gate.entered.notify_waiters();
            self.gate.release.notified().await;
            self.gate.blocked.store(false, Ordering::Release);
        }
        self.outbound
            .send(OutboundMsg::Progress {
                envelope_id: message.envelope_id,
                sequence: message.sequence,
            })
            .await?;
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("progress"));
        Ok(())
    }
}

pub enum BridgeMsg {}

pub struct InboundBridge {
    transport: ChatTransport,
    connect_gate: ConnectGate,
    journal: ActorRef<JournalMsg>,
    router: ActorRef<RouterMsg>,
    evidence: EvidenceTx,
}

impl InboundBridge {
    pub fn new(
        transport: ChatTransport,
        connect_gate: ConnectGate,
        journal: ActorRef<JournalMsg>,
        router: ActorRef<RouterMsg>,
        evidence: EvidenceTx,
    ) -> Self {
        Self {
            transport,
            connect_gate,
            journal,
            router,
            evidence,
        }
    }
}

impl RawActor for InboundBridge {
    type Msg = BridgeMsg;

    fn manual_readiness(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }

    async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        self.connect_gate.wait().await;
        self.transport.connect().await;
        ctx.mark_ready();

        loop {
            tokio::select! {
                command = ctx.recv() => match command {
                    None => {
                        self.evidence.emit(Evidence::ActorStopped("bridge"));
                        return Ok(());
                    }
                    Some(never) => match never {},
                },
                event = self.transport.next_event() => match event {
                    TransportEvent::Disconnected => panic!("scripted chat transport disconnected"),
                    TransportEvent::Message(envelope) => {
                        let inserted = self.journal.call(
                            |reply| JournalMsg::AppendIncoming {
                                envelope_id: envelope.id,
                                chat: envelope.chat.clone(),
                                text: envelope.text.clone(),
                                reply,
                            },
                            CALL_BOUND,
                        ).await?;
                        self.evidence.emit(Evidence::BridgeJournaled {
                            envelope_id: envelope.id,
                            duplicate: !inserted,
                        });

                        if self.transport.ack(envelope.id).await.is_err() {
                            panic!("transport disconnected at the journal-to-ack boundary");
                        }
                        self.router.send(RouterMsg::Incoming(envelope)).await?;
                    }
                }
            }
        }
    }
}
