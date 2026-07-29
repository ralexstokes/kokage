//! Model-client seam and a deterministic scripted implementation.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kokage::CancellationToken;

use crate::messages::{
    ModelAction, ModelError, ModelTurn, ProgressMsg, Role, ToolCall, TurnRequest,
};

pub trait ModelClient: Send + Sync + 'static {
    fn turn(
        &self,
        request: TurnRequest,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ModelTurn, ModelError>> + Send>>;
}

#[derive(Clone, Debug, Default)]
pub struct ScriptedModel {
    rate_limited: Arc<AtomicBool>,
}

impl ScriptedModel {
    pub fn set_rate_limited(&self, value: bool) {
        self.rate_limited.store(value, Ordering::Release);
    }

    pub fn probe(&self) -> bool {
        !self.rate_limited.load(Ordering::Acquire)
    }
}

impl ModelClient for ScriptedModel {
    fn turn(
        &self,
        request: TurnRequest,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ModelTurn, ModelError>> + Send>> {
        let rate_limited = self.rate_limited.clone();
        Box::pin(async move {
            if rate_limited.load(Ordering::Acquire) {
                return Err(ModelError::RateLimited);
            }
            if request.user_text.contains("FLOOD") && request.role == Role::Planner {
                let mut delta = 0_u64;
                loop {
                    if cancel.is_cancelled() {
                        return Err(ModelError::Cancelled);
                    }
                    request
                        .progress
                        .send(ProgressMsg::Delta {
                            chat: request.chat,
                            line: format!("stream delta {delta}"),
                        })
                        .await
                        .map_err(|_| ModelError::Cancelled)?;
                    delta += 1;
                }
            }

            let delay = if request.user_text.contains("SLOW") {
                Duration::from_millis(160)
            } else {
                Duration::from_millis(12)
            };
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(ModelError::Cancelled),
                () = tokio::time::sleep(delay) => {
                    let action = match (request.role, request.turn) {
                        (Role::Planner, _) => ModelAction::Plan(format!(
                            "plan task {} using {} prior turn(s)",
                            request.task,
                            request.attempt
                        )),
                        (Role::Engineer, 0) => ModelAction::Tools(vec![
                            ToolCall {
                                name: "inspect".into(),
                                payload: if request.user_text.contains("STALL-TOOL") {
                                    "stall".into()
                                } else {
                                    "normal".into()
                                },
                            },
                            ToolCall { name: "edit".into(), payload: "normal".into() },
                        ]),
                        (Role::Engineer, _) => ModelAction::Complete("implementation complete".into()),
                        (Role::Reviewer, _) => ModelAction::Review(true),
                    };
                    Ok(ModelTurn { tokens_spent: 10, action })
                }
            }
        })
    }
}
