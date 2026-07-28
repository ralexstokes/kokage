//! Outbound delivery, conflated progress, and the raw inbound bridge.

use std::time::Duration;

use tokio_otp::{
    Actor, ActorContext, ActorRef, ActorResult, DrainPolicy, MessageContext, RawActor,
};

use crate::{
    chat::{ChatEvent, ChatSim},
    messages::{
        InboundMsg, JournalEntry, JournalMsg, OutboundMsg, PHASE_TIMEOUT, ProgressMsg, RouterMsg,
    },
};

#[derive(Clone)]
pub struct Outbound {
    chat: ChatSim,
}

impl Outbound {
    pub fn new(chat: ChatSim) -> Self {
        Self { chat }
    }
}

impl Actor for Outbound {
    type Msg = OutboundMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            OutboundMsg::Reply { chat, text } | OutboundMsg::Notice { chat, text } => {
                self.chat.deliver_reply(chat, text);
            }
        }
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[derive(Clone)]
pub struct Progress {
    chat: ChatSim,
}

impl Progress {
    pub fn new(chat: ChatSim) -> Self {
        Self { chat }
    }
}

impl Actor for Progress {
    type Msg = ProgressMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let (chat, line) = match message {
            ProgressMsg::Delta { chat, line } => (chat, line),
            ProgressMsg::Typing { chat } => (chat, "typing…".to_owned()),
        };
        self.chat.update_progress(chat, line);
        // This models a finite-rate transport and makes mailbox conflation
        // observable during the scripted flood.
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Inbound {
    chat: ChatSim,
    journal: ActorRef<JournalMsg>,
    router: ActorRef<RouterMsg>,
}

impl Inbound {
    pub fn new(chat: ChatSim, journal: ActorRef<JournalMsg>, router: ActorRef<RouterMsg>) -> Self {
        Self {
            chat,
            journal,
            router,
        }
    }

    async fn delivery(&self, delivery: crate::messages::ChatDelivery) -> ActorResult {
        let ack = self
            .journal
            .call(PHASE_TIMEOUT, |reply| JournalMsg::Append {
                chat: delivery.chat,
                entry: JournalEntry::UserMessage {
                    envelope: delivery.envelope,
                    text: delivery.text.clone(),
                },
                reply,
            })
            .await?;
        // Ack-after-append is the durability boundary. A duplicate is also
        // acked but deliberately never reaches the router.
        self.chat.ack(delivery.envelope);
        if !ack.duplicate {
            if delivery.text.eq_ignore_ascii_case("stop") {
                self.router
                    .send(RouterMsg::Stop {
                        chat: delivery.chat,
                    })
                    .await?;
            } else {
                self.router
                    .send(RouterMsg::UserMessage {
                        envelope: delivery.envelope,
                        chat: delivery.chat,
                        text: delivery.text,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn bridge_message(&self, message: InboundMsg) -> ActorResult {
        match message {
            InboundMsg::Delivery(delivery) => self.delivery(delivery).await,
            InboundMsg::Disconnected => {
                self.chat.release_session();
                panic!("scripted inbound disconnect");
            }
        }
    }
}

impl RawActor for Inbound {
    type Msg = InboundMsg;

    fn readiness_gated(&self) -> bool {
        true
    }

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let mut session = self.chat.connect();
        ctx.mark_ready();
        loop {
            tokio::select! {
                biased;
                message = ctx.recv() => match message {
                    Some(message) => {
                        self.bridge_message(message).await?;
                    }
                    None => {
                        self.chat.release_session();
                        return Ok(());
                    }
                },
                event = session.recv() => match event {
                    Some(ChatEvent::Delivery(delivery)) => {
                        self.bridge_message(InboundMsg::Delivery(delivery)).await?;
                    }
                    Some(ChatEvent::Disconnected) | None => {
                        self.bridge_message(InboundMsg::Disconnected).await?;
                    }
                }
            }
        }
    }
}
