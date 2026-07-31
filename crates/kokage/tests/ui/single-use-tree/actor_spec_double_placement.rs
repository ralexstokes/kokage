use kokage::{Actor, ExitResult, ActorSpec, Context, Tree};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {
    let spec = ActorSpec::new("idle", || Idle);
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    tree.add_actor_spec(spec);
}
