use kokage::{ControlError, ScopeRef, TaskSpec};

async fn insertion_does_not_yield_a_lineage(scope: &ScopeRef) -> Result<(), ControlError> {
    let _lineage: u64 = scope
        .add_task(TaskSpec::new("task", |_| async { Ok(()) }))
        .await?;
    Ok(())
}

fn main() {}
