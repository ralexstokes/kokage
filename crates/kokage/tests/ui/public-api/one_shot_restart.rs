use kokage::{OneShotTaskSpec, RestartPolicy};

fn main() {
    let _ = OneShotTaskSpec::new("job", |_| async { Ok(()) }).restart(RestartPolicy::always());
}
