use std::{collections::HashMap, error::Error, time::Duration};

use kokage::{Actor, ActorRef, ActorSpec, Context, DynamicTree, ExitResult, OrderedTree, Reply};
use tokio::sync::mpsc;

enum DirectoryMsg<M> {
    Insert(String, ActorRef<M>),
    Get(String, Reply<Option<ActorRef<M>>>),
}

struct Directory<M> {
    entries: HashMap<String, ActorRef<M>>,
}

impl<M> Clone for Directory<M> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<M: Send + 'static> Actor for Directory<M> {
    type Msg = DirectoryMsg<M>;

    async fn handle(
        &mut self,
        message: DirectoryMsg<M>,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        match message {
            DirectoryMsg::Insert(name, actor_ref) => {
                self.entries.insert(name, actor_ref);
            }
            DirectoryMsg::Get(name, reply) => reply.send(self.entries.get(&name).cloned()),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Printer {
    printed: mpsc::UnboundedSender<String>,
}

impl Actor for Printer {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.printed.send(message).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let directory_spec = ActorSpec::new("directory", || Directory::<String> {
        entries: HashMap::new(),
    });
    let directory = directory_spec.actor_ref();
    let dynamic_tree = DynamicTree::new();
    let dynamic = dynamic_tree.scope();
    let runtime = OrderedTree::new()
        .actor(directory_spec)
        .subtree("dynamic", dynamic_tree)
        .spawn()?;
    let handle = runtime.scope();
    handle.wait_started().await?;

    let (printed, mut output) = mpsc::unbounded_channel();
    let printer = dynamic
        .add_actor(ActorSpec::new("printer", move || Printer {
            printed: printed.clone(),
        }))
        .await?;
    directory
        .send(DirectoryMsg::Insert("receipts".to_owned(), printer))
        .await?;

    let receipts = directory
        .call(Duration::from_secs(1), |reply| {
            DirectoryMsg::Get("receipts".to_owned(), reply)
        })
        .await?
        .expect("receipts printer registered");
    receipts.send("order #42".to_owned()).await?;
    println!("{}", output.recv().await.expect("printed receipt"));

    runtime.shutdown_and_wait().await?;
    Ok(())
}
