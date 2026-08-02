use std::collections::BTreeMap;

use kokage::{Actor, ActorRef, Context, ExitResult, Reply};

use crate::{
    model::{DirectorySnapshot, Key, RouteView},
    shard::ShardMsg,
};

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub view: RouteView,
    pub shard: ActorRef<ShardMsg>,
}

#[derive(Debug)]
pub enum DirectoryMsg {
    Resolve {
        key: Key,
        reply: Reply<Option<Endpoint>>,
    },
    Cutover {
        operation_id: String,
        remove: Vec<String>,
        insert: Vec<Endpoint>,
        reply: Reply<Result<DirectorySnapshot, String>>,
    },
    CutoverStatus {
        operation_id: String,
        reply: Reply<Option<DirectorySnapshot>>,
    },
    Snapshot {
        reply: Reply<DirectorySnapshot>,
    },
}

#[derive(Default)]
pub struct Directory {
    revision: u64,
    planned_rebinds: u64,
    routes: BTreeMap<String, Endpoint>,
    completed_cutovers: BTreeMap<String, CompletedCutover>,
}

#[derive(Clone, Debug)]
struct CompletedCutover {
    remove: Vec<String>,
    insert: Vec<RouteView>,
    snapshot: DirectorySnapshot,
}

impl Directory {
    fn snapshot(&self) -> DirectorySnapshot {
        let mut routes: Vec<_> = self
            .routes
            .values()
            .map(|endpoint| endpoint.view.clone())
            .collect();
        routes.sort_by_key(|route| route.range);
        DirectorySnapshot {
            revision: self.revision,
            planned_rebinds: self.planned_rebinds,
            routes,
        }
    }

    fn cutover(
        &mut self,
        operation_id: String,
        remove: Vec<String>,
        insert: Vec<Endpoint>,
    ) -> Result<DirectorySnapshot, String> {
        let insert_views: Vec<_> = insert
            .iter()
            .map(|endpoint| endpoint.view.clone())
            .collect();
        if let Some(completed) = self.completed_cutovers.get(&operation_id) {
            if completed.remove != remove || completed.insert != insert_views {
                return Err(format!(
                    "directory operation {operation_id} was reused with a different plan"
                ));
            }
            return Ok(completed.snapshot.clone());
        }

        let mut candidate = self.routes.clone();
        for id in &remove {
            if candidate.remove(id).is_none() {
                return Err(format!("directory has no route named {id}"));
            }
        }
        for endpoint in insert {
            let id = endpoint.view.shard_id.clone();
            if candidate.insert(id.clone(), endpoint).is_some() {
                return Err(format!("duplicate directory route {id}"));
            }
        }

        let mut ordered: Vec<_> = candidate.values().collect();
        ordered.sort_by_key(|endpoint| endpoint.view.range);
        for adjacent in ordered.windows(2) {
            if adjacent[0].view.range.end > adjacent[1].view.range.start {
                return Err("directory cutover would create overlapping ranges".to_owned());
            }
        }

        self.routes = candidate;
        self.revision += 1;
        if !remove.is_empty() {
            self.planned_rebinds += 1;
        }
        let snapshot = self.snapshot();
        self.completed_cutovers.insert(
            operation_id,
            CompletedCutover {
                remove,
                insert: insert_views,
                snapshot: snapshot.clone(),
            },
        );
        Ok(snapshot)
    }
}

impl Actor for Directory {
    type Msg = DirectoryMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DirectoryMsg::Resolve { key, reply } => {
                reply.send(
                    self.routes
                        .values()
                        .find(|endpoint| endpoint.view.range.contains(key))
                        .cloned(),
                );
            }
            DirectoryMsg::Cutover {
                operation_id,
                remove,
                insert,
                reply,
            } => reply.send(self.cutover(operation_id, remove, insert)),
            DirectoryMsg::CutoverStatus {
                operation_id,
                reply,
            } => reply.send(
                self.completed_cutovers
                    .get(&operation_id)
                    .map(|completed| completed.snapshot.clone()),
            ),
            DirectoryMsg::Snapshot { reply } => reply.send(self.snapshot()),
        }
        Ok(())
    }
}
