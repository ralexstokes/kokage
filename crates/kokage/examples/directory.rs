use std::{collections::HashMap, error::Error, time::Duration};

use kokage::{
    Actor, ActorRef, ActorResult, DynamicTree, GraphBuilder, MessageContext, OrderedTree, Reply,
};
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
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
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

    async fn handle(
        &mut self,
        message: String,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.printed.send(message).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut graph = GraphBuilder::new();
    let (directory_slot, directory) = graph.slot("directory");
    graph.define(directory_slot, || Directory::<String> {
        entries: HashMap::new(),
    });
    let graph = graph.build()?;
    let handle = OrderedTree::graph(graph)
        .subtree("dynamic", DynamicTree::new())
        .spawn()?;
    handle.wait_started().await?;
    let dynamic = handle
        .subtree("dynamic")
        .expect("dynamic subtree is available");

    let (printed, mut output) = mpsc::unbounded_channel();
    let printer = dynamic
        .dynamic()
        .expect("dynamic scope")
        .add_actor("printer", move || Printer {
            printed: printed.clone(),
        })
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

    handle.shutdown_and_wait().await?;
    Ok(())
}
