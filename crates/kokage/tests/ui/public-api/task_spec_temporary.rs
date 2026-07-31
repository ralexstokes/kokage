use kokage::TaskSpec;

fn main() {
    let _ = TaskSpec::new("job", |_| async { Ok(()) }).temporary();
}
