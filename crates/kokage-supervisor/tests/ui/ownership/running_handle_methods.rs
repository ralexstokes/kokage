use kokage_supervisor::RunningSupervisor;

fn use_handle_methods(running: &RunningSupervisor) {
    let _snapshot = running.snapshot();
}

fn main() {}
