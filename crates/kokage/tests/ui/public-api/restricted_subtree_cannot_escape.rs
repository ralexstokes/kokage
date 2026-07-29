use kokage::{DynamicRestrictedScope, OrderedTree, RuntimeHandle};

async fn escape(scope: DynamicRestrictedScope) -> RuntimeHandle {
    scope
        .add_subtree("nested", OrderedTree::new())
        .await
        .unwrap()
}

fn main() {}
