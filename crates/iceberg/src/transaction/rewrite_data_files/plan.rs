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

use std::collections::HashSet;

use futures::{TryStreamExt, future};

use super::{RewriteDataFilesResult, row_count};
use crate::cow_rewrite::writer::build_replacement_writer;
use crate::spec::DataFile;
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Pins the input files and table state for a row-preserving rewrite.
pub struct RewriteDataFilesPlan {
    base: Table,
    files: Vec<DataFile>,
    sequence_number: i64,
}

impl RewriteDataFilesPlan {
    pub(super) const fn new(base: Table, files: Vec<DataFile>, sequence_number: i64) -> Self {
        Self {
            base,
            files,
            sequence_number,
        }
    }

    /// Returns the exact selected input descriptors.
    #[must_use]
    pub fn input_files(&self) -> &[DataFile] {
        &self.files
    }

    /// Returns the pinned source snapshot identifier.
    #[must_use]
    pub fn base_snapshot_id(&self) -> Option<i64> {
        self.base.metadata().current_snapshot_id()
    }

    /// Writes replacement data files without writing snapshot metadata or publishing.
    ///
    /// # Errors
    /// Returns scan, writer, or record-count validation errors. Partial output
    /// files are retained after errors or cancellation; callers own cleanup.
    pub async fn execute(self) -> Result<RewriteDataFilesResult> {
        if self.files.len() < 2 {
            return Ok(RewriteDataFilesResult::new(
                self.base,
                self.files,
                vec![],
                self.sequence_number,
            ));
        }
        let expected_rows = row_count(&self.files)?;
        let paths: HashSet<_> = self
            .files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        let tasks = self
            .base
            .scan()
            .build()?
            .plan_files()
            .await?
            .try_filter(move |task| future::ready(paths.contains(task.data_file_path())));
        let mut batches = self
            .base
            .reader_builder()
            .build()
            .read(Box::pin(tasks))?
            .stream();
        let mut writer = build_replacement_writer(
            &self.base,
            self.base.metadata().current_schema().clone(),
            None,
        )
        .await?;
        let mut rows = 0_u64;
        while let Some(batch) = batches.try_next().await? {
            rows = rows
                .checked_add(u64::try_from(batch.num_rows())?)
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Compaction row count overflow")
                })?;
            if batch.num_rows() > 0 {
                writer.write(batch).await?;
            }
        }
        let outputs = writer.close().await?;
        if rows != expected_rows || row_count(&outputs)? != expected_rows {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Compaction row count does not match source descriptors",
            ));
        }
        Ok(RewriteDataFilesResult::new(
            self.base,
            self.files,
            outputs,
            self.sequence_number,
        ))
    }
}
