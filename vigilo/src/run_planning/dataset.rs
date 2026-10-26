//! Height-bounded verified dataset traversal. Every descent checks the exact
//! advertised range, kind, level and owner; only one case batch is returned.
use anyhow::{
    Result,
    ensure,
};

use super::Session;
use crate::contracts::case_storage::*;
pub(super) struct DatasetCursor {
    root: Option<IndexDescriptor>,
    stack: Vec<(IndexPage, usize, u64)>,
    resident_bytes: u64,
    next: i32,
    end: i32,
}
impl DatasetCursor {
    pub(super) fn new(manifest: &DatasetManifest) -> Self {
        Self {
            root: Some(manifest.batches.clone()),
            stack: Vec::new(),
            resident_bytes: 0,
            next: 0,
            end: manifest.case_count,
        }
    }
    pub(super) async fn next(
        &mut self,
        session: &Session<'_>,
    ) -> Result<Option<(IndexEntry, Vec<u8>)>> {
        if let Some(root) = self.root.take() {
            ensure!(
                root.object.byte_count <= 4 * 1024 * 1024,
                "index working set exceeds 4 MiB"
            );
            let bytes = session.read(&root.object).await?;
            let page: IndexPage = decode(&root.object.verify(&bytes, session.limits)?)?;
            root.validate_page(&page, IndexKind::DatasetBatches, session.limits)?;
            self.resident_bytes = root.object.byte_count;
            self.stack.push((page, 0, root.object.byte_count));
        }
        while let Some((page, index, size)) = self.stack.last_mut() {
            if *index == page.entries.len() {
                self.resident_bytes -= *size;
                self.stack.pop();
                continue;
            }
            let entry = page.entries[*index].clone();
            *index += 1;
            if page.level > 0 {
                ensure!(
                    self.resident_bytes + entry.object.byte_count <= 4 * 1024 * 1024,
                    "index working set exceeds 4 MiB"
                );
            }
            let bytes = session.read(&entry.object).await?;
            if page.level > 0 {
                let child: IndexPage = decode(&entry.object.verify(&bytes, session.limits)?)?;
                page.validate_child(*index - 1, &child, session.limits)?;
                self.resident_bytes += entry.object.byte_count;
                self.stack.push((child, 0, entry.object.byte_count));
            } else {
                ensure!(
                    entry.range_start == self.next,
                    "dataset traversal has a gap or overlap"
                );
                self.next = entry.range_end;
                return Ok(Some((entry, bytes)));
            }
        }
        ensure!(
            self.next == self.end,
            "dataset index does not cover every case"
        );
        Ok(None)
    }
}
