use crate::supervisor::{ChildSpec, Supervisor, TaskSpec, private};

fn waiting_child(id: &str) -> TaskSpec {
    TaskSpec::new(id, |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
}

#[tokio::test]
async fn attached_children_walk_direct_memberships_before_descendants() {
    let nested = Supervisor::ordered()
        .child_spec(
            waiting_child("leaf")
                .into_spec()
                .attachment("leaf metadata".to_owned()),
        )
        .build()
        .expect("nested supervisor builds");
    let root_owner = Supervisor::ordered()
        .child_spec(
            waiting_child("worker")
                .into_spec()
                .attachment("worker metadata".to_owned()),
        )
        .child_spec(
            ChildSpec::supervisor("branch", nested).attachment("branch metadata".to_owned()),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = root_owner.handle();
    root.wait_started().await.expect("tree starts");

    let attached = private::attached_children::<String>(&root);
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
    let handle_owner = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let handle = handle_owner.handle();
    let old_lineage = handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child_spec(
            waiting_child("worker")
                .into_spec()
                .attachment("old".to_owned()),
        )
        .await
        .expect("old child added");

    let old = private::attached_children::<String>(&handle);
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].attachment().as_str(), "old");
    assert_eq!(old[0].path()[0].lineage, old_lineage);

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("worker")
        .await
        .expect("old child removed");
    let new_lineage = handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child_spec(
            waiting_child("worker")
                .into_spec()
                .attachment("new".to_owned()),
        )
        .await
        .expect("replacement child added");

    let current = private::attached_children::<String>(&handle);
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].attachment().as_str(), "new");
    assert_eq!(current[0].path()[0].lineage, new_lineage);
    assert_ne!(new_lineage, old_lineage);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn attachments_are_absent_from_serialized_snapshots() {
    let handle_owner = Supervisor::ordered()
        .child_spec(
            waiting_child("worker")
                .into_spec()
                .attachment("not serialized".to_owned()),
        )
        .build()
        .expect("supervisor builds")
        .spawn();
    let handle = handle_owner.handle();
    handle.wait_started().await.expect("supervisor starts");

    let snapshot = serde_json::to_string(&handle.snapshot()).expect("snapshot serializes");
    assert!(!snapshot.contains("not serialized"));

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
