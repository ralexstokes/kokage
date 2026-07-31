use kokage::{Actor as ActorTrait, ActorSpec, Context, ExitResult, RunningTree, TaskSpec, Tree};

struct Actor;

impl ActorTrait for Actor {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

async fn membership_operations_require_scope_ref(running: &RunningTree) {
    let _ = running
        .add_actor_spec(ActorSpec::new("actor", || Actor))
        .await;
    let _ = running
        .add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }))
        .await;
    let _ = running.add_subtree("tree", Tree::new()).await;
    let _ = running.remove_child("child").await;
}

async fn observation_and_control_require_scope_ref(running: &RunningTree) {
    let _ = running.kind();
    let _ = running.subtree("tree");
    let _ = running.snapshot();
    let _ = running.snapshots();
    let _ = running.observe_children();
    let _ = running.lifecycle_events();
    let _ = running.actor_stats();
    let _ = running.wait_started().await;
}

fn main() {}
