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

use std::collections::HashMap;

use futures::TryStreamExt;

use super::group::group_files;
use super::{RewriteDataFilesGroup, RewriteDataFilesResult};
use crate::Result;
use crate::scan::FileScanTask;
use crate::spec::DataFile;
use crate::table::Table;

/// Pins the input files and partition groups for a row-preserving rewrite.
pub struct RewriteDataFilesPlan {
    base: Table,
    files: Vec<DataFile>,
    groups: Vec<RewriteDataFilesGroup>,
}

impl RewriteDataFilesPlan {
    pub(super) fn new(
        base: Table,
        files: Vec<DataFile>,
        sequences: &HashMap<String, i64>,
    ) -> Result<Self> {
        let groups = group_files(&base, &files, sequences)?;
        Ok(Self {
            base,
            files,
            groups,
        })
    }

    /// Returns the selected descriptors, including singleton groups left untouched.
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
    /// Groups run sequentially without mixing partitions or specs. Singleton
    /// groups write no files and do not appear in the result's replaced inputs.
    ///
    /// # Errors
    /// Returns scan, writer, or record-count validation errors. Partial output
    /// files are retained after errors or cancellation; callers own cleanup.
    pub async fn execute(self) -> Result<RewriteDataFilesResult> {
        let tasks = self.tasks().await?;
        let mut inputs = vec![];
        let mut outputs = vec![];
        let mut sequences = HashMap::new();
        for (group, tasks) in self.groups.iter().zip(tasks) {
            if group.files.len() < 2 {
                continue;
            }
            let files = group.execute(&self.base, tasks).await?;
            sequences.extend(
                files
                    .iter()
                    .map(|file| (file.file_path().to_string(), group.sequence_number)),
            );
            outputs.extend(files);
            inputs.extend(group.files.iter().cloned());
        }
        Ok(RewriteDataFilesResult::new(
            self.base, inputs, outputs, sequences,
        ))
    }

    async fn tasks(&self) -> Result<Vec<Vec<FileScanTask>>> {
        let mut tasks: Vec<Vec<FileScanTask>> = self.groups.iter().map(|_| vec![]).collect();
        let paths: HashMap<_, _> = self
            .groups
            .iter()
            .enumerate()
            .filter(|(_, group)| group.files.len() >= 2)
            .flat_map(|(index, group)| {
                group
                    .files
                    .iter()
                    .map(move |file| (file.file_path(), index))
            })
            .collect();
        if paths.is_empty() {
            return Ok(tasks);
        }
        let mut stream = self.base.scan().build()?.plan_files().await?;
        while let Some(task) = stream.try_next().await? {
            if let Some(index) = paths.get(task.data_file_path()).copied() {
                tasks[index].push(task);
            }
        }
        Ok(tasks)
    }
}
