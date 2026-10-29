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

#![forbid(unsafe_code)]
#![deny(warnings, dead_code, unused, missing_docs)]
#![deny(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
#![deny(clippy::all, clippy::pedantic, clippy::nursery)]
#![deny(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::excessive_nesting,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::undocumented_unsafe_blocks,
    clippy::as_conversions,
    clippy::wildcard_imports
)]

mod group;
mod plan;
mod prepared;
mod result;
mod validation;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
pub use group::RewriteDataFilesGroup;
pub use plan::RewriteDataFilesPlan;
pub use prepared::PreparedRewriteDataFiles;
pub use result::RewriteDataFilesResult;

use super::action::CommitStatus;
use super::overwrite::OverwriteFilesState;
use super::snapshot::{current_manifests, validate_partition_value};
use super::{ActionCommit, TransactionAction};
use crate::spec::{DataFile, FormatVersion, ManifestContentType};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Plans row-preserving compaction against a pinned table state.
///
/// Supports main on v2 tables without live delete files. Inputs are grouped by
/// partition spec and partition value; singleton groups are left untouched.
/// Output rolling uses the table's target Parquet file size.
/// Completed outputs can be prepared against append-only descendants. Other
/// planning-base changes require replanning. Files are never deleted
/// automatically, including after failed or ambiguous publication.
///
/// ```no_run
/// use iceberg::table::Table;
/// use iceberg::transaction::Transaction;
/// use iceberg::{Catalog, Result};
///
/// async fn compact(table: &Table, catalog: &dyn Catalog) -> Result<()> {
///     let plan = Transaction::new(table).rewrite_data_files().plan().await?;
///     let result = plan.execute().await?;
///     let fresh = catalog.load_table(table.identifier()).await?;
///     if let Some(prepared) = result.prepare(&fresh).await? {
///         catalog
///             .update_table(prepared.try_into_table_commit()?)
///             .await?;
///     }
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct RewriteDataFilesAction {
    base: Table,
    selected: Option<Vec<DataFile>>,
}

impl RewriteDataFilesAction {
    pub(crate) fn new(table: &Table) -> Self {
        Self {
            base: table.clone(),
            selected: None,
        }
    }

    /// Attaches a cooperative cancellation signal to all rewrite stages.
    #[must_use]
    pub fn with_cancellation(mut self, token: crate::io::CancellationToken) -> Self {
        self.base = self.base.with_cancellation(token);
        self
    }

    /// Restricts compaction to exact live data-file descriptors.
    #[must_use]
    pub fn select_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.selected = Some(files.into_iter().collect());
        self
    }

    /// Resolves the selected inputs without writing files or publishing metadata.
    ///
    /// # Errors
    /// Returns an error for unsupported tables, live deletes, duplicate inputs,
    /// or selected descriptors that do not match live files.
    pub async fn plan(&self) -> Result<RewriteDataFilesPlan> {
        let (files, sequence_numbers) = self.inputs(&self.base).await?;
        let files = self.select_inputs(files)?;
        RewriteDataFilesPlan::new(self.base.clone(), files, &sequence_numbers)
    }

    fn select_inputs(&self, files: Vec<DataFile>) -> Result<Vec<DataFile>> {
        let Some(selected) = &self.selected else {
            return Ok(files);
        };
        let live: HashMap<_, _> = files.iter().map(|file| (file.file_path(), file)).collect();
        let mut paths = HashSet::new();
        for file in selected {
            if !paths.insert(file.file_path()) || live.get(file.file_path()).copied() != Some(file)
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Selected compaction input is duplicate, missing, or has a mismatched descriptor",
                ));
            }
        }
        Ok(selected.clone())
    }

    async fn inputs(&self, table: &Table) -> Result<(Vec<DataFile>, HashMap<String, i64>)> {
        self.base.file_io().check_cancellation()?;
        validate_base(&self.base, table)?;
        if self.base.metadata().format_version() != FormatVersion::V2 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Compaction currently requires format v2",
            ));
        }
        let mut files = vec![];
        let mut sequence_numbers = HashMap::new();
        let mut paths = HashSet::new();
        for descriptor in current_manifests(table).await? {
            self.base.file_io().check_cancellation()?;
            let manifest = table.object_cache().get_manifest(&descriptor).await?;
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                if descriptor.content != ManifestContentType::Data {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Compaction with live delete files is not yet supported",
                    ));
                }
                let file = entry.data_file();
                let spec = self
                    .base
                    .metadata()
                    .partition_spec_by_id(file.partition_spec_id)
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Unknown compaction partition spec")
                    })?;
                validate_partition_value(
                    file.partition(),
                    &spec.partition_type(self.base.metadata().current_schema())?,
                )?;
                if !paths.insert(entry.file_path().to_string()) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Duplicate live compaction input path",
                    ));
                }
                let sequence = entry.sequence_number().filter(|n| *n >= 0).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Compaction input lacks a valid data sequence number",
                    )
                })?;
                sequence_numbers.insert(entry.file_path().to_string(), sequence);
                files.push(entry.data_file().clone());
            }
        }
        Ok((files, sequence_numbers))
    }
}

/// Retry-persistent replacement files and publication state.
pub struct RewriteDataFilesState {
    prepared: Option<(RewriteDataFilesResult, OverwriteFilesState)>,
}

#[async_trait]
impl TransactionAction for RewriteDataFilesAction {
    type State = RewriteDataFilesState;

    fn new_state(&self) -> Self::State {
        RewriteDataFilesState { prepared: None }
    }

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        self.base.file_io().check_cancellation()?;
        validate_base(&self.base, table)?;
        if state.prepared.is_none() {
            let result = self.plan().await?.execute().await?;
            let publication = result.new_state();
            state.prepared = Some((result, publication));
        }
        let (result, publication) = state.prepared.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Compaction publication was not prepared",
            )
        })?;
        result.commit(publication, table).await
    }

    async fn cleanup(self, _state: Self::State, _table: &Table, _status: CommitStatus) {}
}

fn validate_base(base: &Table, table: &Table) -> Result<()> {
    if base.readonly() || table.readonly() {
        return Err(Error::new(
            ErrorKind::PreconditionFailed,
            "Cannot compact a read-only table",
        ));
    }
    if base.identifier() != table.identifier()
        || base.metadata() != table.metadata()
        || base.metadata_location() != table.metadata_location()
    {
        return Err(Error::new(
            ErrorKind::PreconditionFailed,
            "Compaction planning base changed; replan the rewrite",
        ));
    }
    Ok(())
}

fn row_count(files: &[DataFile]) -> Result<u64> {
    files.iter().try_fold(0_u64, |sum, file| {
        sum.checked_add(file.record_count())
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Compaction row count overflow"))
    })
}

#[cfg(test)]
mod tests;
