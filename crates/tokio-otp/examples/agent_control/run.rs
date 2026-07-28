//! One transient role run, implemented as a mailbox-driven state machine.

use std::sync::Arc;
use tokio_otp::LiveContext;

use tokio_otp::{Actor, ActorRef, ActorResult, CancellationToken, MessageContext, StartContext};

use crate::{
    messages::{
        BudgetMsg, ChatId, EffectStatus, JournalEntry, JournalMsg, MODEL_DEADLINE, ModelAction,
        ModelError, PHASE_TIMEOUT, ProgressMsg, Role, RunMsg, RunOutput, SessionMsg, TOOL_DEADLINE,
        TaskId, ToolCall, ToolHostMsg, ToolOutcome, TurnRequest,
    },
    model::ModelClient,
};

#[derive(tokio_otp::ActorFactory)]
pub struct AgentRun {
    chat: ChatId,
    task: TaskId,
    role: Role,
    attempt: u64,
    user_text: String,
    model: Arc<dyn ModelClient>,
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    tool_host: ActorRef<ToolHostMsg>,
    progress: ActorRef<ProgressMsg>,
    session: ActorRef<SessionMsg>,
    cancel: CancellationToken,
    #[factory(default)]
    turn: u64,
    #[factory(default)]
    tools: Vec<ToolCall>,
}

impl AgentRun {
    async fn append(&self, entry: JournalEntry) -> ActorResult {
        self.journal
            .call(PHASE_TIMEOUT, |reply| JournalMsg::Append {
                chat: self.chat,
                entry,
                reply,
            })
            .await?;
        Ok(())
    }

    fn start_model(&self, ctx: &mut impl LiveContext<RunMsg>) {
        let request = TurnRequest {
            chat: self.chat,
            task: self.task,
            role: self.role,
            attempt: self.attempt,
            turn: self.turn,
            user_text: self.user_text.clone(),
            progress: self.progress.clone(),
        };
        let model = self.model.clone();
        let cancel = self.cancel.clone();
        ctx.offload_or(
            MODEL_DEADLINE,
            model.turn(request, cancel),
            Err(ModelError::Deadline),
            |result| RunMsg::ModelResult { result },
        );
    }

    async fn start_tool(&self, index: usize, ctx: &mut impl LiveContext<RunMsg>) -> ActorResult {
        let call = self.tools[index].clone();
        let key = format!("{}:{}:{index}", self.chat, self.task);
        self.append(JournalEntry::ToolIntent {
            task: self.task,
            key: key.clone(),
            call: call.name.clone(),
        })
        .await?;
        let tool_host = self.tool_host.clone();
        let offload_key = key.clone();
        ctx.offload_or(
            TOOL_DEADLINE + PHASE_TIMEOUT,
            async move {
                let execute = tool_host
                    .call(TOOL_DEADLINE, |reply| ToolHostMsg::Execute {
                        key: offload_key.clone(),
                        call,
                        reply,
                    })
                    .await;
                match execute {
                    Ok(outcome) => outcome,
                    _ => {
                        // A timeout is an unknown outcome. Querying is ordered
                        // behind the in-flight Execute in the tool-host mailbox,
                        // so this deterministically reconciles the completed key.
                        match tool_host
                            .call(PHASE_TIMEOUT, |reply| ToolHostMsg::Query {
                                key: offload_key,
                                reply,
                            })
                            .await
                        {
                            Ok(EffectStatus::Found(outcome)) => outcome,
                            _ => ToolOutcome {
                                output: "tool outcome remained unknown".into(),
                            },
                        }
                    }
                }
            },
            ToolOutcome {
                output: "tool outcome remained unknown".into(),
            },
            move |result| RunMsg::ToolResult { index, key, result },
        );
        Ok(())
    }

    async fn finish(&self, ctx: &mut MessageContext<'_, Self>, output: RunOutput) -> ActorResult {
        self.session
            .send(SessionMsg::RunFinished {
                task: self.task,
                role: self.role,
                output,
            })
            .await?;
        ctx.stop();
        Ok(())
    }
}

impl Actor for AgentRun {
    type Msg = RunMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        ctx.continue_with(RunMsg::Step);
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            RunMsg::Step => self.start_model(ctx),
            RunMsg::ModelResult { result } => {
                let turn = match result {
                    Ok(turn) => turn,
                    Err(ModelError::Cancelled) => {
                        self.append(JournalEntry::Checkpoint {
                            task: self.task,
                            state: "model step cancelled".into(),
                        })
                        .await?;
                        return self.finish(ctx, RunOutput::Cancelled).await;
                    }
                    Err(ModelError::RateLimited | ModelError::Deadline) => {
                        self.append(JournalEntry::Checkpoint {
                            task: self.task,
                            state: "retryable model failure".into(),
                        })
                        .await?;
                        self.finish(ctx, RunOutput::RetryableFailure).await?;
                        return Err(std::io::Error::other("model provider unavailable").into());
                    }
                };
                self.budget
                    .send(BudgetMsg::Charge {
                        chat: self.chat,
                        tokens: turn.tokens_spent,
                    })
                    .await?;
                self.progress
                    .send(ProgressMsg::Delta {
                        chat: self.chat,
                        line: format!("{:?} turn {} complete", self.role, self.turn),
                    })
                    .await?;
                match turn.action {
                    ModelAction::Plan(plan) => {
                        self.append(JournalEntry::Plan {
                            task: self.task,
                            text: plan.clone(),
                        })
                        .await?;
                        return self.finish(ctx, RunOutput::Planned(plan)).await;
                    }
                    ModelAction::Tools(tools) => {
                        self.tools = tools;
                        self.start_tool(0, ctx).await?;
                    }
                    ModelAction::Complete(output) => {
                        return self.finish(ctx, RunOutput::Engineered(output)).await;
                    }
                    ModelAction::Review(approved) => {
                        self.append(JournalEntry::Review {
                            task: self.task,
                            approved,
                        })
                        .await?;
                        return self.finish(ctx, RunOutput::Reviewed(approved)).await;
                    }
                }
            }
            RunMsg::ToolResult { index, key, result } => {
                self.append(JournalEntry::ToolEffect {
                    task: self.task,
                    key,
                    outcome: result,
                })
                .await?;
                if self.user_text.contains("PANIC-MIDRUN")
                    && self.role == Role::Engineer
                    && self.attempt == 0
                    && index == 0
                {
                    panic!("scripted engineer panic after tool effect");
                }
                let next = index + 1;
                if next < self.tools.len() {
                    self.start_tool(next, ctx).await?;
                } else {
                    self.turn += 1;
                    ctx.continue_with(RunMsg::Step);
                }
            }
        }
        Ok(())
    }
}
