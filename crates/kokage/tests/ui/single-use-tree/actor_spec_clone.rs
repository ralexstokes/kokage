use kokage::{Actor, ActorResult, ActorSpec, Context};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(
        &mut self,
        (): (),
        _ctx: &mut Context<'_, Self>,
    ) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let spec = ActorSpec::new("idle", || Idle);
    let _copy = spec.clone();
}
