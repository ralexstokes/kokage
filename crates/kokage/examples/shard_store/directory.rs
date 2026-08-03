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
    routes: BTreeMap<Key, Endpoint>,
    // Bounded by the acceptance script; production must expire operation ids
    // only after callers can no longer retry an unknown cutover outcome.
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

        let previous_extent = self
            .routes
            .first_key_value()
            .zip(self.routes.last_key_value())
            .map(|((_, first), (_, last))| (first.view.range.start, last.view.range.end));
        let mut candidate = self.routes.clone();
        for id in &remove {
            let start = candidate
                .iter()
                .find_map(|(start, endpoint)| (endpoint.view.shard_id == *id).then_some(*start))
                .ok_or_else(|| format!("directory has no route named {id}"))?;
            candidate.remove(&start);
        }
        for endpoint in insert {
            let id = endpoint.view.shard_id.clone();
            if candidate
                .values()
                .any(|existing| existing.view.shard_id == id)
            {
                return Err(format!("duplicate directory route {id}"));
            }
            let start = endpoint.view.range.start;
            if candidate.insert(start, endpoint).is_some() {
                return Err(format!("duplicate directory range start {start}"));
            }
        }

        let ordered: Vec<_> = candidate.values().collect();
        if ordered.is_empty() {
            return Err("directory cutover would remove every route".to_owned());
        }
        for adjacent in ordered.windows(2) {
            if adjacent[0].view.range.end > adjacent[1].view.range.start {
                return Err("directory cutover would create overlapping ranges".to_owned());
            }
            if adjacent[0].view.range.end < adjacent[1].view.range.start {
                return Err("directory cutover would create a coverage gap".to_owned());
            }
        }
        if let Some((previous_start, previous_end)) = previous_extent {
            let next_start = ordered[0].view.range.start;
            let next_end = ordered
                .last()
                .expect("non-empty directory candidate")
                .view
                .range
                .end;
            if (next_start, next_end) != (previous_start, previous_end) {
                return Err("directory cutover would change the covered key extent".to_owned());
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
                        .range(..=key)
                        .next_back()
                        .map(|(_, endpoint)| endpoint)
                        .filter(|endpoint| endpoint.view.range.contains(key))
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
