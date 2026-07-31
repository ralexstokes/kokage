//! Token-spend metering with a global cap and guard notification.

use kokage::{Actor, ActorRef, Context, ExitResult};

use crate::messages::{BudgetMsg, BudgetReport, GuardMsg};

#[derive(kokage::ActorFactory)]
pub struct Budget {
    guard: ActorRef<GuardMsg>,
    #[factory(default = BudgetReport {
        cap: u64::MAX,
        ..BudgetReport::default()
    })]
    report: BudgetReport,
}

impl Actor for Budget {
    type Msg = BudgetMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
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
