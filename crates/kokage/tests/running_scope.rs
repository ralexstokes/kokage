use kokage::{
    DynamicScopeRef, DynamicTree, RunningScope, RunningTree, ScopeRef, SupervisorError, Tree,
};

struct ManagedTree<S: RunningScope> {
    tree: RunningTree<S>,
}

fn manage<S: RunningScope>(tree: RunningTree<S>) -> ManagedTree<S> {
    ManagedTree { tree }
}

impl<S: RunningScope> ManagedTree<S> {
    fn scope(&self) -> S {
        self.tree.scope()
    }

    async fn shutdown(self) -> Result<(), SupervisorError> {
        self.tree.shutdown().await
    }
}

#[tokio::test]
async fn downstream_generic_wrapper_accepts_both_running_scope_types() {
    let ordered = manage(Tree::new().spawn().expect("ordered tree spawns"));
    let _: ScopeRef = ordered.scope();
    ordered.shutdown().await.expect("ordered tree stops");

    let dynamic = manage(DynamicTree::new().spawn().expect("dynamic tree spawns"));
    let _: DynamicScopeRef = dynamic.scope();
    dynamic.shutdown().await.expect("dynamic tree stops");
}
