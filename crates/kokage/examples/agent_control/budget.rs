//! Token-spend metering with a global cap and guard notification.

use kokage::{Actor, ActorRef, ActorResult, MessageContext};

use crate::messages::{BudgetMsg, BudgetReport, GuardMsg};

pub struct Budget {
    guard: ActorRef<GuardMsg>,
    report: BudgetReport,
}

impl Budget {
    pub fn new(guard: ActorRef<GuardMsg>) -> Self {
        Self {
            guard,
            report: BudgetReport {
                cap: u64::MAX,
                ..BudgetReport::default()
            },
        }
    }
}

impl Actor for Budget {
    type Msg = BudgetMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            BudgetMsg::Charge { chat, tokens } => {
                *self.report.by_chat.entry(chat).or_default() += tokens;
                self.report.total += tokens;
                if self.report.total > self.report.cap {
                    self.guard.send(GuardMsg::BudgetExceeded).await?;
                }
            }
            BudgetMsg::SetGlobalCap { tokens } => self.report.cap = tokens,
            BudgetMsg::UnderCap { reply } => reply.send(self.report.total <= self.report.cap),
            BudgetMsg::Report { reply } => reply.send(self.report.clone()),
        }
        Ok(())
    }
}
