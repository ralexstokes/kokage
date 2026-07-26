//! The build executor: one action at a time, on the blocking pool.
//!
//! Workers are created and destroyed at runtime by the pool leader, so their
//! configuration cannot come from a registration closure written at startup.
//! `#[derive(ActorFactory)]` gives that configuration a name — `WorkerFactory`
//! is built once by the leader and cloned into every incarnation the dynamic
//! scope spawns — while the per-incarnation counters are marked
//! `#[factory(default)]` and reset on restart. The split is exactly the one an
//! executor wants: shared handles and shared accounting survive a crashed
//! compile, "how much did *this* run do" does not.

use std::sync::Arc;

use tokio_otp::{
    Actor, ActorFactory, ActorRef, ActorResult, BoxError, DrainPolicy, LiveContext, MessageContext,
    StopContext, prelude::Continue,
};

use crate::{
    messages::{
        Artifact, CALL_DEADLINE, CasMsg, ExecOutcome, Phase, ProgressMsg, TargetProgress, WorkerMsg,
    },
    plan::{CHUNKS, TargetId, artifact_bytes, compile_chunk},
    shared::{AttemptLog, BuildJournal, WorkerExit},
};

/// Executes one action at a time against the shared store.
#[derive(ActorFactory)]
pub struct Worker {
    cas: ActorRef<CasMsg>,
    progress: ActorRef<ProgressMsg>,
    attempts: Arc<AttemptLog>,
    journal: Arc<BuildJournal>,
    #[factory(default)]
    built: u64,
    #[factory(default)]
    cached: u64,
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
        let target = action.target;

        // A worker has exactly one job in flight, so waiting inline for the
        // store is not head-of-line blocking in any meaningful sense: there is
        // no queued traffic this call could delay that is not already waiting
        // on this same action. Contrast the pool, which must pipeline.
        if let Some(artifact) = self
            .cas
            .call(CALL_DEADLINE, |reply| CasMsg::Lookup { digest, reply })
            .await?
        {
            self.cached += 1;
            self.note(target, Phase::Cached).await;
            reply.send(ExecOutcome::Cached(artifact));
            return Ok(Continue);
        }

        // Claiming the attempt before running anything that can panic is what
        // bounds the retry loop: the pool requeues every lost dispatch, and a
        // poison target loses all of them.
        let Some(attempt) = self.attempts.begin(target) else {
            self.note(target, Phase::Failed).await;
            reply.send(ExecOutcome::Quarantined {
                attempts: self.attempts.spent(target),
            });
            return Ok(Continue);
        };

        let mut hash = 0;
        for chunk in 0..CHUNKS {
            let slice = action.clone();
            // Two shapes of "did not finish", and neither is this worker's
            // failure: `Err(BlockingCancelled)` means the slice never reached a
            // blocking thread, `Ok(None)` means the closure saw the token fire
            // partway through. Either way this worker is being retired or the
            // graph is going down. Dropping `reply` is the signal — the pool's
            // pipelined dispatch resolves as `Lost` and requeues the target.
            let Ok(Some(partial)) = ctx
                .run_blocking(move |cancel| compile_chunk(&slice, attempt, chunk, cancel))
                .await
            else {
                return Ok(Continue);
            };
            hash ^= partial;
            self.note(target, Phase::Running(percent(chunk))).await;
        }

        let artifact = Artifact {
            target,
            digest,
            bytes: artifact_bytes(hash),
        };
        let _ = self.cas.send(CasMsg::Store { artifact }).await;
        self.built += 1;
        self.note(target, Phase::Built).await;
        reply.send(ExecOutcome::Built(artifact));
        Ok(Continue)
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_worker(WorkerExit {
            label: ctx.id().to_owned(),
            built: self.built,
            cached: self.cached,
        });
        Ok(())
    }

    /// A retired or crashed worker's queued actions are better re-dispatched
    /// to a live worker than drained by one that is on its way out.
    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Discard
    }
}

impl Worker {
    /// Posts a progress update, tolerating a display that has already stopped.
    ///
    /// Returning `Err` from `handle` *fails* the actor, so a send that races
    /// sibling shutdown cannot be propagated with `?` — a display update is
    /// never worth turning a clean stop into a supervised failure. There is no
    /// blanket "best effort" send, so the decision is repeated at each call
    /// site by hand.
    async fn note(&self, target: TargetId, phase: Phase) {
        let _ = self
            .progress
            .send(ProgressMsg::Update(TargetProgress { target, phase }))
            .await;
    }
}

fn percent(chunk: u32) -> u8 {
    u8::try_from((u64::from(chunk) + 1) * 100 / u64::from(CHUNKS)).unwrap_or(100)
}
