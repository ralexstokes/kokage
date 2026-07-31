use kokage::{ScopeRef, TaskSpec, Tree};

async fn mutation_requires_dynamic_scope(scope: &ScopeRef) {
    let _ = scope
        .add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }))
        .await;
    let _ = scope.add_subtree("tree", Tree::new()).await;
    let _ = scope.remove_child("child").await;
}

fn main() {}
