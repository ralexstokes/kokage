use std::time::Duration;

use crate::supervisor::{Backoff, BuildError, RestartPolicy, Supervisor, TaskSpec};

fn restart_with_backoff(backoff: Backoff) -> RestartPolicy {
    RestartPolicy::on_failure()
        .limit(1, Duration::from_secs(1))
        .backoff(backoff)
}

#[test]
fn empty_children_are_accepted() {
    Supervisor::ordered()
        .build()
        .expect("building without children should succeed");
}

#[test]
fn duplicate_child_ids_are_rejected() {
    let err = Supervisor::ordered()
        .child(TaskSpec::new("dup", |_| async { Ok(()) }))
        .child(TaskSpec::new("dup", |_| async { Ok(()) }))
        .build()
        .expect_err("duplicate child ids must be rejected");

    assert!(matches!(err, BuildError::DuplicateChildId(id) if id == "dup"));
}

#[test]
fn invalid_restart_intensity_is_rejected() {
    let err = Supervisor::ordered()
        .default_child_restart(RestartPolicy::on_failure().limit(1, Duration::ZERO))
        .child(TaskSpec::new("worker", |_| async { Ok(()) }))
        .build()
        .expect_err("zero-width restart windows should be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn invalid_jittered_restart_intensity_is_rejected() {
    let err = Supervisor::ordered()
        .default_child_restart(restart_with_backoff(Backoff::exponential_with_jitter(
            Duration::ZERO,
            2,
            Duration::from_millis(10),
        )))
        .child(TaskSpec::new("worker", |_| async { Ok(()) }))
        .build()
        .expect_err("invalid jittered exponential backoff should be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn invalid_fixed_backoff_delay_is_rejected() {
    let err = Supervisor::ordered()
        .default_child_restart(restart_with_backoff(Backoff::fixed(Duration::ZERO)))
        .child(TaskSpec::new("worker", |_| async { Ok(()) }))
        .build()
        .expect_err("zero fixed backoff delay should be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn invalid_exponential_restart_factor_is_rejected() {
    let err = Supervisor::ordered()
        .default_child_restart(restart_with_backoff(Backoff::exponential(
            Duration::from_millis(10),
            0,
            Duration::from_millis(20),
        )))
        .child(TaskSpec::new("worker", |_| async { Ok(()) }))
        .build()
        .expect_err("zero exponential factor should be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn invalid_exponential_restart_max_is_rejected() {
    let err = Supervisor::ordered()
        .default_child_restart(restart_with_backoff(Backoff::exponential(
            Duration::from_millis(10),
            2,
            Duration::ZERO,
        )))
        .child(TaskSpec::new("worker", |_| async { Ok(()) }))
        .build()
        .expect_err("zero exponential max should be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn invalid_child_restart_intensity_is_rejected() {
    let err = Supervisor::ordered()
        .child(
            TaskSpec::new("worker", |_| async { Ok(()) })
                .restart(RestartPolicy::on_failure().limit(1, Duration::ZERO)),
        )
        .build()
        .expect_err("zero-width child restart windows should be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn empty_child_id_is_rejected() {
    let err = Supervisor::ordered()
        .child(TaskSpec::new("", |_| async { Ok(()) }))
        .build()
        .expect_err("empty child id must be rejected");

    assert!(matches!(err, BuildError::InvalidConfig(_)));
}

#[test]
fn valid_configuration_builds() {
    let supervisor = Supervisor::ordered()
        .child(TaskSpec::new("worker", |_| async { Ok(()) }))
        .build();

    assert!(supervisor.is_ok(), "expected valid configuration to build");
}
