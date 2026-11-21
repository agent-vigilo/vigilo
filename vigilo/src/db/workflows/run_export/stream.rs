//! Bounded ordered merge of physical readers for the operator JSONL stream.
//!
//! A window owns at most four readers. Each retains one root page and four child
//! buffers; no transaction survives a refill. Selecting the smallest logical key
//! preserves route/execution grouping even when aliases alternate between shards.
use std::collections::VecDeque;

use serde_json::Value;
use uuid::Uuid;

use super::{
    ChildPages,
    Execution,
    RunExportGroup,
    load_children,
    select_group_execution_page,
};

pub(crate) struct ExecutionStream {
    group: RunExportGroup,
    cursor: Option<Value>,
    more: bool,
    roots: VecDeque<Execution>,
    children: Option<ChildPages>,
    remaining_groups: VecDeque<RunExportGroup>,
}
impl ExecutionStream {
    fn new(group: RunExportGroup) -> Self {
        Self {
            group,
            cursor: None,
            more: true,
            roots: VecDeque::new(),
            children: None,
            remaining_groups: VecDeque::new(),
        }
    }
    async fn peek(&mut self, run: Uuid, limit: i64) -> anyhow::Result<Option<&Execution>> {
        while self.roots.is_empty() {
            if !self.more {
                let Some(group) = self.remaining_groups.pop_front() else {
                    return Ok(None);
                };
                self.group = group;
                self.cursor = None;
                self.more = true;
                self.children = None;
            }
            let page =
                select_group_execution_page(&self.group, run, self.cursor.as_ref(), limit).await?;
            self.cursor = page.cursor;
            self.more = page.has_more;
            if !page.executions.is_empty() {
                let parents = page
                    .executions
                    .iter()
                    .map(|e| (e.run_shard, e.id))
                    .collect::<Vec<_>>();
                self.children = Some(load_children(&self.group, run, &parents, limit).await?);
            }
            self.roots = page.executions.into();
        }
        Ok(self.roots.front())
    }
    /// The last selected execution belongs to this page's child buffers.
    pub(crate) fn current(&mut self) -> (&str, &mut ChildPages) {
        (
            self.group.database_alias(),
            self.children.as_mut().expect("selected execution page"),
        )
    }
}

/// Keep one stream per physical alias. Bounded shard groups on the same
/// database are consumed sequentially, without buffering later groups early.
pub(crate) fn streams_for_window(groups: Vec<RunExportGroup>) -> Vec<ExecutionStream> {
    let mut streams: Vec<ExecutionStream> = Vec::new();
    let mut indices = std::collections::BTreeMap::<String, usize>::new();
    for group in groups {
        if let Some(&i) = indices.get(&group.database_alias) {
            streams[i].remaining_groups.push_back(group);
        } else {
            indices.insert(group.database_alias.clone(), streams.len());
            streams.push(ExecutionStream::new(group));
        }
    }
    streams
}

/// Refills exhausted buffers, then removes the smallest logical execution key.
/// At most one bounded refill runs at a time; row iteration itself performs no I/O.
pub(crate) async fn next_execution(
    streams: &mut [ExecutionStream],
    run: Uuid,
    limit: i64,
) -> anyhow::Result<Option<(usize, Execution)>> {
    let mut chosen: Option<(usize, (i16, Uuid))> = None;
    for (i, stream) in streams.iter_mut().enumerate() {
        if let Some(e) = stream.peek(run, limit).await? {
            let key = (e.run_shard, e.id);
            if chosen.is_none_or(|(_, previous)| key < previous) {
                chosen = Some((i, key));
            }
        }
    }
    Ok(chosen.map(|(i, _)| (i, streams[i].roots.pop_front().expect("peeked execution"))))
}
