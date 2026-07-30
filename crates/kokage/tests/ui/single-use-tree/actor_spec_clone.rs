use kokage::{Actor, ExitResult, ActorSpec, Context};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(
        &mut self,
        (): (),
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        Ok(())
    }
}

fn main() {
    let spec = ActorSpec::new("idle", || Idle);
    let _copy = spec.clone();
}
