#![cfg(feature = "serde")]

use kokage::{RestartPolicy, ScopeChange, observe::LifecycleEvent};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/public-wire-shapes.json"))
        .expect("public wire-shape fixtures are valid JSON")
}

fn assert_golden<T>(fixture: &Value)
where
    T: DeserializeOwned + Serialize,
{
    let decoded: T =
        serde_json::from_value(fixture.clone()).expect("fixture matches the public wire type");
    assert_eq!(
        serde_json::to_value(decoded).expect("public wire type serializes"),
        *fixture
    );
}

#[test]
fn restart_policy_wire_shape_is_pinned() {
    assert_golden::<RestartPolicy>(&fixtures()["restart_policy"]);
}

#[test]
fn lifecycle_event_wire_shape_is_pinned() {
    assert_golden::<LifecycleEvent>(&fixtures()["lifecycle_event"]);
}

#[test]
fn scope_change_wire_shape_is_pinned() {
    assert_golden::<ScopeChange>(&fixtures()["scope_change"]);
}
