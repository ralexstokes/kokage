use kokage::{
    Actor, ActorResult, ActorSpec, DynamicRuntime, DynamicTree, Context, OrderedTree,
    host::ChildSpec,
};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }
}

async fn mutate(runtime: DynamicRuntime) {
    let actor = ActorSpec::new("actor", || Idle);
    let child = ChildSpec::task("task", |_| async { Ok(()) });
    let _ = runtime.add_actor(actor).await;
    let _ = runtime.add_child(child).await;
    let _ = runtime.add_subtree("ordered", OrderedTree::new()).await;
    let _ = runtime.add_subtree("dynamic", DynamicTree::new()).await;
    let _ = runtime.remove_child("child").await;
}

fn main() {}
