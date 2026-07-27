//! Runtime-created build executor.

use std::sync::Arc;

use tokio_otp::{
    Actor, ActorFactory, ActorRef, ActorResult, DrainPolicy, LiveContext, MessageContext,
    prelude::Continue,
};

use crate::{
    messages::{Artifact, CALL_DEADLINE, CasMsg, ExecOutcome, Phase, ProgressMsg, WorkerMsg},
    model::compile,
    shared::AttemptBook,
};

#[derive(ActorFactory)]
pub struct Worker {
    cas: ActorRef<CasMsg>,
    progress: ActorRef<ProgressMsg>,
    attempts: Arc<AttemptBook>,
}

impl Actor for Worker {
    type Msg = WorkerMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        let WorkerMsg::Execute {
            action,
            digest,
            reply,
        } = message;

        if let Some(artifact) = self
            .cas
            .call(CALL_DEADLINE, |reply| CasMsg::Lookup { digest, reply })
            .await?
        {
            self.note(action.target, Phase::Cached).await;
            reply.send(ExecOutcome::Cached(artifact));
            return Ok(Continue);
        }

        let attempt = self.attempts.begin(action.target);
        self.note(action.target, Phase::Running).await;
        let target = action.target;
        let compiled = action.clone();
        let Ok(Some(bytes)) = ctx
            .run_blocking(move |cancellation| compile(&compiled, attempt, cancellation))
            .await
        else {
            return Ok(Continue);
        };
        let artifact = Artifact {
            target,
            digest,
            bytes,
        };
        let _ = self.cas.send(CasMsg::Store(artifact)).await;
        self.note(target, Phase::Built).await;
        reply.send(ExecOutcome::Built(artifact));
        Ok(Continue)
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Discard
    }
}

impl Worker {
    async fn note(&self, target: &'static str, phase: Phase) {
        let _ = self
            .progress
            .send(ProgressMsg::Update { target, phase })
            .await;
    }
}
