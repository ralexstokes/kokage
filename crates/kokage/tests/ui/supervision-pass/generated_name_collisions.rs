use kokage::{Actor, ActorResult, GraphBuilder, MessageContext, Supervision};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct CollisionNames {
    builder: Worker,
    wire: Worker,
    refs: Worker,
    f0: Worker,
}

fn main() {
    let mut builder = GraphBuilder::new();
    let refs = CollisionNames::wire(&mut builder, |_refs| CollisionNamesFactories {
        builder: || Worker,
        wire: || Worker,
        refs: || Worker,
        f0: || Worker,
    });
    let _wire = refs.wire.clone();
    builder.build().expect("collision-name graph builds");
}
