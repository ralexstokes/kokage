use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, time::timeout};
use tokio_supervisor::{
    ChildSpec, ChildStateView, RestartIntensity, RestartPolicy, SupervisorBuilder, SupervisorSpec,
};

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
        .supervisor(
            "branch",
            SupervisorSpec::new(nested).attachment("branch metadata".to_owned()),
        )
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
    let handle = SupervisorBuilder::new()
        .build()
        .expect("supervisor builds")
        .spawn();
    let old_epoch = handle
        .add_child(waiting_child("worker").attachment("old".to_owned()))
        .await
        .expect("old child added");

    let old = handle.attached_children::<String>();
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].attachment().as_str(), "old");
    assert_eq!(old[0].path()[0].membership_epoch, old_epoch);

    handle
        .remove_child("worker")
        .await
        .expect("old child removed");
    let new_epoch = handle
        .add_child(waiting_child("worker").attachment("new".to_owned()))
        .await
        .expect("replacement child added");

    let current = handle.attached_children::<String>();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].attachment().as_str(), "new");
    assert_eq!(current[0].path()[0].membership_epoch, new_epoch);
    assert_ne!(new_epoch, old_epoch);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reincarnation_never_exposes_a_displaced_nested_attachment_identity() {
    let crash = Arc::new(Notify::new());
    let crash_for_child = Arc::clone(&crash);
    let static_slot = SupervisorBuilder::new()
        .child(waiting_child("static-leaf").attachment("static leaf".to_owned()))
        .build()
        .expect("static slot builds");
    let middle = SupervisorBuilder::new()
        .supervisor(
            "slot",
            SupervisorSpec::new(static_slot).attachment("static slot".to_owned()),
        )
        .child(
            ChildSpec::new("bomb", move |_ctx| {
                let crash = Arc::clone(&crash_for_child);
                async move {
                    crash.notified().await;
                    Err(std::io::Error::other("middle failed").into())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("middle builds");
    let root = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(32, Duration::from_secs(60))),
        )
        .build()
        .expect("root builds")
        .spawn();
    root.wait_started().await.expect("tree starts");

    for expected_generation in 1..=16 {
        let middle = root.supervisor("middle").expect("middle handle");
        middle
            .remove_child("slot")
            .await
            .expect("static slot removed");
        let dynamic_slot = SupervisorBuilder::new()
            .child(waiting_child("dynamic-leaf").attachment("dynamic leaf".to_owned()))
            .build()
            .expect("dynamic slot builds");
        middle
            .add_supervisor(
                "slot",
                SupervisorSpec::new(dynamic_slot).attachment("dynamic slot".to_owned()),
            )
            .await
            .expect("same-id dynamic slot added");

        let sampling = Arc::new(AtomicBool::new(true));
        let sampler_root = root.clone();
        let sampler_sampling = Arc::clone(&sampling);
        let sampler = tokio::spawn(async move {
            while sampler_sampling.load(Ordering::Relaxed) {
                for attached in sampler_root.attached_children::<String>() {
                    let Some(middle_identity) = attached.path().first() else {
                        continue;
                    };
                    if middle_identity.id != "middle"
                        || middle_identity.generation < expected_generation
                    {
                        continue;
                    }
                    assert_ne!(
                        attached.attachment().as_str(),
                        "dynamic leaf",
                        "the new middle incarnation descended through the displaced dynamic slot"
                    );
                    if attached.attachment().as_str() == "static slot" {
                        let slot_snapshot = attached
                            .supervisor()
                            .expect("slot attachment carries a supervisor handle")
                            .snapshot();
                        assert!(
                            slot_snapshot.child("dynamic-leaf").is_none(),
                            "the initial static attachment used the displaced dynamic stable identity"
                        );
                    }
                }
                tokio::task::yield_now().await;
            }
        });

        let mut lifecycle = root.watch_lifecycle();
        let baseline = root
            .snapshot()
            .child("middle")
            .expect("middle child exists")
            .generation;
        crash.notify_one();
        timeout(
            Duration::from_secs(1),
            lifecycle.wait_started("middle", baseline),
        )
        .await
        .expect("middle restart completed in time")
        .expect("root remains live");

        timeout(Duration::from_secs(1), async {
            loop {
                let current = root.attached_children::<String>();
                if current.iter().any(|attached| {
                    attached.attachment().as_str() == "static leaf"
                        && attached.path()[0].generation == expected_generation
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconciled static attachment became visible");
        sampling.store(false, Ordering::Relaxed);
        sampler.await.expect("attachment sampler completed");

        assert!(root.snapshot().child("middle").is_some_and(|child| {
            child.generation == expected_generation && child.state == ChildStateView::Running
        }));
    }

    root.shutdown_and_wait().await.expect("clean shutdown");
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
