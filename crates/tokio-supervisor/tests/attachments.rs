use tokio_supervisor::{ChildSpec, DynamicSupervisorBuilder, SupervisorBuilder, SupervisorSpec};

fn waiting_child(id: &str) -> ChildSpec {
    ChildSpec::new(id, |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
}

#[tokio::test]
async fn attached_children_walk_direct_memberships_before_descendants() {
    let nested = SupervisorBuilder::new()
        .child(waiting_child("leaf").attachment("leaf metadata".to_owned()))
        .build()
        .expect("nested supervisor builds");
    let root = SupervisorBuilder::new()
        .child(waiting_child("worker").attachment("worker metadata".to_owned()))
        .supervisor(SupervisorSpec::new("branch", nested).attachment("branch metadata".to_owned()))
        .build()
        .expect("root supervisor builds")
        .spawn();
    root.wait_started().await.expect("tree starts");

    let attached = root.attached_children::<String>();
    let values = attached
        .iter()
        .map(|child| child.attachment().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        ["worker metadata", "branch metadata", "leaf metadata"]
    );
    assert_eq!(attached[0].path()[0].id, "worker");
    assert_eq!(attached[1].path()[0].id, "branch");
    assert!(attached[1].supervisor().is_some());
    assert_eq!(
        attached[2]
            .path()
            .iter()
            .map(|identity| identity.id.as_str())
            .collect::<Vec<_>>(),
        ["branch", "leaf"]
    );
    assert!(attached[2].supervisor().is_none());

    root.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn replacing_a_child_replaces_its_attachment_and_identity_atomically() {
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("supervisor builds")
        .spawn();
    let old_lineage = handle
        .add_child(waiting_child("worker").attachment("old".to_owned()))
        .await
        .expect("old child added");

    let old = handle.attached_children::<String>();
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].attachment().as_str(), "old");
    assert_eq!(old[0].path()[0].lineage, old_lineage);

    handle
        .remove_child("worker")
        .await
        .expect("old child removed");
    let new_lineage = handle
        .add_child(waiting_child("worker").attachment("new".to_owned()))
        .await
        .expect("replacement child added");

    let current = handle.attached_children::<String>();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].attachment().as_str(), "new");
    assert_eq!(current[0].path()[0].lineage, new_lineage);
    assert_ne!(new_lineage, old_lineage);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn attachments_are_absent_from_serialized_snapshots() {
    let handle = SupervisorBuilder::new()
        .child(waiting_child("worker").attachment("not serialized".to_owned()))
        .build()
        .expect("supervisor builds")
        .spawn();
    handle.wait_started().await.expect("supervisor starts");

    let snapshot = serde_json::to_string(&handle.snapshot()).expect("snapshot serializes");
    assert!(!snapshot.contains("not serialized"));

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
