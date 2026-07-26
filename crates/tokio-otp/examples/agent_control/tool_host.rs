//! Idempotent tool execution with blocking-pool isolation and reconciliation.

use std::{collections::HashMap, time::Duration};

use tokio_otp::{Actor, ActorContext, ActorResult, prelude::Continue};

use crate::messages::{EffectStatus, ToolHostMsg, ToolOutcome, ToolReport};

#[derive(Default)]
pub struct ToolHost {
    effects: HashMap<String, ToolOutcome>,
    counts: HashMap<String, usize>,
    queries: HashMap<String, usize>,
}

impl Actor for ToolHost {
    type Msg = ToolHostMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut ActorContext<Self::Msg>,
    ) -> ActorResult {
        match message {
            ToolHostMsg::Execute { key, call, reply } => {
                if let Some(outcome) = self.effects.get(&key).cloned() {
                    reply.send(outcome);
                    return Ok(Continue);
                }
                let name = call.name.clone();
                let stalled = call.payload == "stall";
                let outcome = ctx
                    .run_blocking(move |_| {
                        std::thread::sleep(if stalled {
                            Duration::from_millis(400)
                        } else {
                            Duration::from_millis(4)
                        });
                        ToolOutcome {
                            output: format!("{name} effect complete"),
                        }
                    })
                    .await?;
                self.effects.insert(key.clone(), outcome.clone());
                *self.counts.entry(key).or_default() += 1;
                reply.send(outcome);
            }
            ToolHostMsg::Query { key, reply } => {
                *self.queries.entry(key.clone()).or_default() += 1;
                reply.send(
                    self.effects
                        .get(&key)
                        .cloned()
                        .map_or(EffectStatus::Missing, EffectStatus::Found),
                );
            }
            ToolHostMsg::Report { reply } => reply.send(ToolReport {
                effects: self.counts.clone(),
                queries: self.queries.clone(),
            }),
        }
        Ok(Continue)
    }
}
