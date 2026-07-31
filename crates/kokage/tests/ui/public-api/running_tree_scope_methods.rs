use kokage::{
    Actor as ActorTrait, ActorSpec, Context, ExitResult, OrderedTree, RunningTree,
    TaskSpec,
};

struct Actor;

impl ActorTrait for Actor {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

async fn scope_operations_require_scope_ref(running: &RunningTree) {
    let _ = running.kind();
    let _ = running.snapshot();
    let _ = running.wait_started().await;
    let _ = running.completions(["child"]);
    let _ = running.watch_lifecycle();
    let _ = running.actor_stats();
    let _ = running.subtree("child");
    let _ = running
        .add_actor(ActorSpec::new("actor", || Actor))
        .await;
    let _ = running
        .add_task(TaskSpec::new("task", |_| async { Ok(()) }))
        .await;
    let _ = running.add_subtree("tree", OrderedTree::new()).await;
    let _ = running.remove_child("child").await;
}

fn main() {}
