use kokage::{DynamicRuntimeHandle, RuntimeHandle};

fn removed_runtime_handle_methods(handle: &RuntimeHandle) {
    let _ = handle.wait_completed_dynamic(["future"]);
    let _ = handle.shutdown_on_dynamic_completion(["future"]);
}

fn removed_dynamic_handle_conversions(handle: DynamicRuntimeHandle) {
    let _ = handle.as_runtime_handle();
    let _: RuntimeHandle = handle.into();
}

fn main() {}
