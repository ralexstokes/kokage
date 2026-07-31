use kokage::{Actor, ActorFactory, Context, ExitResult};

#[allow(non_camel_case_types)]
#[derive(kokage::ActorFactory)]
struct __KokageActorFactoryDefaults {
    #[factory(default = 7)]
    local: usize,
}

impl Actor for __KokageActorFactoryDefaults {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {
    let actor = __KokageActorFactoryDefaultsFactory {}.build();
    assert_eq!(actor.local, 7);
}
