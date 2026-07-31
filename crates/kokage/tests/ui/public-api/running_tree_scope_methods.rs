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

fn main() {}
