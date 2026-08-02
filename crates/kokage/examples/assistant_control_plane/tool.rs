use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{Actor, Context, ExitResult, Reply, StopContext};
use tokio::sync::Mutex;

use crate::common::{Evidence, EvidenceTx};

#[derive(Default)]
pub struct ToolState {
    results: Mutex<HashMap<String, String>>,
    executions: Mutex<HashMap<String, u64>>,
    blocking_runs: AtomicU64,
}

impl ToolState {
    pub async fn executions(&self, key: &str) -> u64 {
        self.executions
            .lock()
            .await
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    pub fn blocking_runs(&self) -> u64 {
        self.blocking_runs.load(Ordering::Acquire)
    }
}

pub enum ToolMsg {
    Execute {
        key: String,
        stall: bool,
        reply: Reply<String>,
    },
    Query {
        key: String,
        reply: Reply<Option<String>>,
    },
}

pub struct ToolHost {
    state: Arc<ToolState>,
    evidence: EvidenceTx,
    generation: u64,
}

impl ToolHost {
    pub fn new(state: Arc<ToolState>, evidence: EvidenceTx, generation: u64) -> Self {
        Self {
            state,
            evidence,
            generation,
        }
    }
}

impl Actor for ToolHost {
    type Msg = ToolMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.evidence.emit(Evidence::ActorStarted {
            actor: "tool-host",
            generation: self.generation,
        });
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ToolMsg::Execute { key, stall, reply } => {
                if let Some(result) = self.state.results.lock().await.get(&key).cloned() {
                    reply.send(result);
                    return Ok(());
                }

                let blocking_runs = Arc::clone(&self.state);
                let effect_key = key.clone();
                let result = ctx
                    .run_blocking(move |cancel| {
                        blocking_runs.blocking_runs.fetch_add(1, Ordering::AcqRel);
                        if stall {
                            std::thread::sleep(Duration::from_millis(80));
                        }
                        if cancel.is_cancelled() {
                            return format!("cancelled:{effect_key}");
                        }
                        format!("effect:{effect_key}")
                    })
                    .await?;

                self.state
                    .results
                    .lock()
                    .await
                    .insert(key.clone(), result.clone());
                *self.state.executions.lock().await.entry(key).or_default() += 1;
                reply.send(result);
            }
            ToolMsg::Query { key, reply } => {
                reply.send(self.state.results.lock().await.get(&key).cloned());
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("tool-host"));
        Ok(())
    }
}
