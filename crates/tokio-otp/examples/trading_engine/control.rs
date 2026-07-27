use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures_util::future::join_all;
use tokio_otp::prelude::*;

use crate::messages::{CALL_DEADLINE, ControlMsg, GatewayMsg};

#[derive(Clone)]
pub struct Control {
    pub gateways: Vec<ActorRef<GatewayMsg>>,
    pub intake_gate: Arc<AtomicBool>,
}

impl Control {
    async fn cancel_gateway(gateway: ActorRef<GatewayMsg>) -> usize {
        gateway
            .call(CALL_DEADLINE, |reply| GatewayMsg::CancelAll { reply })
            .await
            .unwrap_or_default()
    }

    async fn cancel_all(&self) -> usize {
        join_all(self.gateways.iter().cloned().map(Self::cancel_gateway))
            .await
            .into_iter()
            .sum()
    }
}

impl Actor for Control {
    type Msg = ControlMsg;

    async fn handle(
        &mut self,
        message: ControlMsg,
        _ctx: &mut MessageContext<'_, ControlMsg>,
    ) -> ActorResult {
        match message {
            ControlMsg::KillSwitch { reply } => {
                self.intake_gate.store(false, Ordering::Release);
                let cancelled_orders = self.cancel_all().await;
                tracing::warn!(cancelled_orders, "kill switch cancelled open orders");
                reply.send(());
            }
            ControlMsg::EmergencyCancelAll { reply } => reply.send(self.cancel_all().await),
        }
        Ok(Continue)
    }
}
