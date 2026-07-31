use kokage::{Actor as ActorTrait, ActorSpec, Context, ExitResult, RunningTree, TaskSpec, Tree};

struct Actor;

impl ActorTrait for Actor {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

async fn membership_operations_require_scope_ref(running_tree: &RunningTree) {
    let _ = running_tree
        .add_actor_spec(ActorSpec::new("actor", || Actor))
        .await;
    let _ = running_tree
        .add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }))
        .await;
    let _ = running_tree.add_subtree("tree", Tree::new()).await;
    let _ = running_tree.remove_child("child").await;
}

async fn observation_and_control_require_scope_ref(running_tree: &RunningTree) {
    let _ = running_tree.kind();
    let _ = running_tree.subtree("tree");
    let _ = running_tree.observe_children();
    let _ = running_tree.lifecycle_events();
    let _ = running_tree.actor_stats();
}

fn main() {}
