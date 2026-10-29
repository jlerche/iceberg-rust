// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};

use futures::{TryStreamExt, future};

use super::group::group_files;
use super::{RewriteDataFilesGroup, RewriteDataFilesResult};
use crate::scan::FileScanTaskStream;
use crate::spec::DataFile;
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Pins the input files and partition groups for a row-preserving rewrite.
pub struct RewriteDataFilesPlan {
    base: Table,
    files: Vec<DataFile>,
    groups: Vec<RewriteDataFilesGroup>,
    sequences: HashMap<String, i64>,
    has_deletes: bool,
}

impl RewriteDataFilesPlan {
    pub(super) fn new(
        base: Table,
        files: Vec<DataFile>,
        sequences: &HashMap<String, i64>,
        has_deletes: bool,
    ) -> Result<Self> {
        let base = base.track_outputs(crate::io::ArtifactInventory::default());
        // Materialized equality deletes must not reapply to newer surviving rows.
        let sequences = sequences
            .iter()
            .map(|(path, sequence)| {
                (
                    path.clone(),
                    if has_deletes {
                        base.metadata().last_sequence_number()
                    } else {
                        *sequence
                    },
                )
            })
            .collect();
        let groups = group_files(&base, &files, &sequences)?;
        Ok(Self {
            base,
            files,
            groups,
            sequences,
            has_deletes,
        })
    }

    /// Splits groups deterministically within file-count and source-byte limits.
    ///
    /// # Errors
    /// Rejects fewer than two files per group, zero bytes, or any oversized input.
    /// Singleton remainders are untouched only on tables without live deletes.
    /// Limits cover source descriptors,
    /// not compressed output size or the memory occupied by a decoded batch.
    pub fn with_group_limits(mut self, max_files: usize, max_bytes: u64) -> Result<Self> {
        if max_files < 2 || max_bytes == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite limits require at least two files and nonzero bytes",
            ));
        }
        self.groups = self
            .groups
            .iter()
            .map(|group| group.split(max_files, max_bytes, &self.sequences))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(self)
    }

    /// Returns a shared output inventory to retain across execution failures.
    #[must_use]
    pub fn artifacts(&self) -> crate::io::ArtifactInventory {
        self.base.file_io().inventory().unwrap_or_default()
    }

    /// Returns the selected data-file descriptors, including singleton groups.
    #[must_use]
    pub fn input_files(&self) -> &[DataFile] {
        &self.files
    }

    /// Returns rewrite groups in first-input order, including singleton groups.
    #[must_use]
    pub fn groups(&self) -> &[RewriteDataFilesGroup] {
        &self.groups
    }

    /// Returns the pinned source snapshot identifier.
    #[must_use]
    pub fn base_snapshot_id(&self) -> Option<i64> {
        self.base.metadata().current_snapshot_id()
    }

    /// Writes replacement data files without writing snapshot metadata or publishing.
    ///
    /// Groups run sequentially without mixing partitions or specs. Tables with
    /// live deletes also rewrite singleton groups. Delete files remain live;
    /// their effects are materialized into replacement rows.
    ///
    /// # Errors
    /// Returns scan, writer, or record-count validation errors. Partial output
    /// files are retained after errors or cancellation; callers own cleanup.
    pub async fn execute(self) -> Result<RewriteDataFilesResult> {
        self.base.file_io().check_cancellation()?;
        let mut inputs = vec![];
        let mut outputs = vec![];
        let mut sequences = HashMap::new();
        for group in &self.groups {
            self.base.file_io().check_cancellation()?;
            if group.files.len() < 2 && !self.has_deletes {
                continue;
            }
            let tasks = self.tasks(group).await?;
            let files = group.execute(&self.base, tasks).await?;
            sequences.extend(
                files
                    .iter()
                    .map(|file| (file.file_path().to_string(), group.sequence_number)),
            );
            outputs.extend(files);
            inputs.extend(group.files.iter().cloned());
        }
        self.base.file_io().check_cancellation()?;
        Ok(RewriteDataFilesResult::new(
            self.base, inputs, outputs, sequences,
        ))
    }

    async fn tasks(&self, group: &RewriteDataFilesGroup) -> Result<FileScanTaskStream> {
        let paths: HashSet<String> = group
            .files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        let tasks = self
            .base
            .scan()
            .with_concurrency_limit(1)
            .with_data_file_concurrency_limit(1)
            .with_manifest_entry_concurrency_limit(1)
            .build()?
            .plan_files()
            .await?
            .try_filter(move |task| future::ready(paths.contains(task.data_file_path())));
        Ok(Box::pin(tasks))
    }
}
