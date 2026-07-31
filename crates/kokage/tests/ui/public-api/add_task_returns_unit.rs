use kokage::{ControlError, DynamicScopeRef, TaskSpec};

async fn insertion_does_not_yield_a_lineage(scope: &DynamicScopeRef) -> Result<(), ControlError> {
    let _lineage: u64 = scope
        .add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }))
        .await?;
    Ok(())
}

fn main() {}
