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

use async_trait::async_trait;

use super::super::action::CommitStatus;
use super::super::overwrite::{OverwriteFilesAction, OverwriteFilesState};
use super::super::{ActionCommit, TransactionAction};
use super::PreparedRewriteDataFiles;
use super::validation::validate_publication;
use crate::Result;
use crate::spec::DataFile;
use crate::table::Table;

/// Holds completed immutable replacements that have not been published.
#[derive(Clone)]
pub struct RewriteDataFilesResult {
    base: Table,
    inputs: Vec<DataFile>,
    outputs: Vec<DataFile>,
    sequence_numbers: HashMap<String, i64>,
}

impl RewriteDataFilesResult {
    pub(super) const fn new(
        base: Table,
        inputs: Vec<DataFile>,
        outputs: Vec<DataFile>,
        sequence_numbers: HashMap<String, i64>,
    ) -> Self {
        Self {
            base,
            inputs,
            outputs,
            sequence_numbers,
        }
    }

    /// Returns outputs allocated during execution and subsequent preparation.
    #[must_use]
    pub fn artifacts(&self) -> crate::io::ArtifactInventory {
        self.base.file_io().inventory().unwrap_or_default()
    }

    /// Returns the source files replaced by completed rewrite groups.
    #[must_use]
    pub fn input_files(&self) -> &[DataFile] {
        &self.inputs
    }

    /// Returns the newly written immutable replacement files.
    #[must_use]
    pub fn output_files(&self) -> &[DataFile] {
        &self.outputs
    }

    /// Returns whether execution produced a publishable rewrite.
    #[must_use]
    pub const fn has_changes(&self) -> bool {
        !self.inputs.is_empty()
    }

    /// Writes snapshot metadata and returns a standard commit without publishing.
    ///
    /// Supply a freshly loaded table. This method does not load or update a
    /// catalog. Returns `None` for a validated no-op. The result can also be
    /// applied to a transaction; that path never rewrites data files. Append-only
    /// descendants are accepted when all original files and their data sequences
    /// remain unchanged, the live delete inventory is unchanged, and the writer
    /// configuration is unchanged.
    ///
    /// # Errors
    /// Returns an error for incompatible planning-base changes, invalid input
    /// liveness, or metadata-writing failure. No staged files are deleted on failure.
    pub async fn prepare(&self, table: &Table) -> Result<Option<PreparedRewriteDataFiles>> {
        self.base.file_io().check_cancellation()?;
        if !self.has_changes() {
            validate_publication(&self.base, table).await?;
            return Ok(None);
        }
        let mut state = self.new_state();
        let mut tracked = table.clone().track_outputs(self.artifacts());
        if let Some(token) = self.base.file_io().cancellation() {
            tracked = tracked.with_cancellation(token);
        }
        let commit = self.commit(&mut state, &tracked).await?;
        PreparedRewriteDataFiles::new(tracked, commit).map(Some)
    }

    fn action(&self, table: &Table) -> OverwriteFilesAction {
        OverwriteFilesAction::for_rewrite(table, self.sequence_numbers.clone())
            .remove_data_files(self.inputs.clone())
            .add_data_files(self.outputs.clone())
    }
}

#[async_trait]
impl TransactionAction for RewriteDataFilesResult {
    type State = OverwriteFilesState;

    fn new_state(&self) -> Self::State {
        self.action(&self.base).new_state()
    }

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        self.base.file_io().check_cancellation()?;
        validate_publication(&self.base, table).await?;
        if !self.has_changes() {
            return Ok(ActionCommit::new(vec![], vec![])
                .with_cancellation(self.base.file_io().cancellation()));
        }
        let mut tracked = table.clone().track_outputs(self.artifacts());
        if let Some(token) = self.base.file_io().cancellation() {
            tracked = tracked.with_cancellation(token);
        }
        let commit = self.action(&tracked).commit(state, &tracked).await?;
        self.base.file_io().check_cancellation()?;
        Ok(commit
            .with_artifacts(self.artifacts())
            .with_cancellation(self.base.file_io().cancellation()))
    }

    async fn cleanup(self, _state: Self::State, _table: &Table, _status: CommitStatus) {}
}
