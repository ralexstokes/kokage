use kokage::{OrderedTree, RestrictedScopeRef, ScopeRef};

async fn escape(scope: RestrictedScopeRef) -> ScopeRef {
    scope
        .add_subtree("nested", OrderedTree::new())
        .await
        .unwrap()
}

fn main() {}
