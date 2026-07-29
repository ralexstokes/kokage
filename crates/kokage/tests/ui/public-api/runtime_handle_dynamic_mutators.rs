use kokage::{
    Actor, ActorResult, ActorSpec, Context, DynamicTree, RuntimeHandle,
};
use kokage::host::ChildSpec;

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

fn universal_handles_cannot_mutate(handle: &RuntimeHandle) {
    let _ = handle.add_actor(ActorSpec::new("actor", || Idle));
    let _ = handle.add_actor_with("actor", || Idle);
    let _ = handle.add_child(ChildSpec::task("task", |_| async { Ok(()) }));
    let _ = handle.add_subtree("subtree", DynamicTree::new());
    let _ = handle.remove_child("child");
}

fn main() {}
