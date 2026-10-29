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

use std::collections::HashSet;

use async_trait::async_trait;
use futures::TryStreamExt;

use super::action::CommitStatus;
use super::overwrite::{OverwriteFilesAction, OverwriteFilesState};
use super::snapshot::current_manifests;
use super::{ActionCommit, TransactionAction};
use crate::cow_rewrite::writer::build_replacement_writer;
use crate::spec::{DataFile, FormatVersion, ManifestContentType, TableMetadataRef};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Compacts all current data files while preserving every row and its multiplicity.
///
/// Supports main on unpartitioned v2 tables without live delete files, with all
/// inputs under the default spec. Output uses the table's target Parquet file
/// size; file count reduction depends on that setting. Fewer than two inputs
/// produces no snapshot. Any change to the planning base requires replanning.
/// Output data sequence numbers use the minimum input sequence number; file
/// sequence numbers are assigned by the new replace snapshot. Physical files
/// are retained for historical reads, including after failed or ambiguous commits.
///
/// ```no_run
/// use iceberg::table::Table;
/// use iceberg::transaction::{ApplyTransactionAction, Transaction};
/// use iceberg::{Catalog, Result};
///
/// async fn compact(table: &Table, catalog: &dyn Catalog) -> Result<Table> {
///     let tx = Transaction::new(table);
///     tx.rewrite_data_files().apply(tx)?.commit(catalog).await
/// }
/// ```
#[derive(Clone)]
pub struct RewriteDataFilesAction {
    base: TableMetadataRef,
}

impl RewriteDataFilesAction {
    pub(crate) fn new(table: &Table) -> Self {
        Self {
            base: table.metadata_ref(),
        }
    }

    async fn inputs(&self, table: &Table) -> Result<(Vec<DataFile>, i64)> {
        if table.metadata() != self.base.as_ref() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Compaction planning base changed; replan the rewrite",
            ));
        }
        if self.base.format_version() != FormatVersion::V2
            || !self.base.default_partition_spec().fields().is_empty()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Compaction currently requires an unpartitioned v2 table",
            ));
        }
        let mut files = vec![];
        let mut sequence_number = i64::MAX;
        let mut paths = HashSet::new();
        for descriptor in current_manifests(table).await? {
            let manifest = table.object_cache().get_manifest(&descriptor).await?;
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                if descriptor.content != ManifestContentType::Data {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Compaction with live delete files is not yet supported",
                    ));
                }
                if entry.data_file().partition_spec_id != self.base.default_partition_spec_id() {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Compaction requires all live data files to use the default partition spec",
                    ));
                }
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
                sequence_number = sequence_number.min(sequence);
                files.push(entry.data_file().clone());
            }
        }
        Ok((files, sequence_number))
    }
}

/// Retry-persistent replacement files and publication state.
pub struct RewriteDataFilesState {
    prepared: Option<(OverwriteFilesAction, OverwriteFilesState)>,
}

#[async_trait]
impl TransactionAction for RewriteDataFilesAction {
    type State = RewriteDataFilesState;

    fn new_state(&self) -> Self::State {
        RewriteDataFilesState { prepared: None }
    }

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        let (files, sequence_number) = self.inputs(table).await?;
        if files.len() < 2 {
            return Ok(ActionCommit::new(vec![], vec![]));
        }
        if state.prepared.is_none() {
            let expected_rows = row_count(&files)?;
            let mut batches = table.scan().build()?.to_arrow().await?;
            let mut writer =
                build_replacement_writer(table, self.base.current_schema().clone(), None).await?;
            let mut rows = 0_u64;
            while let Some(batch) = batches.try_next().await? {
                rows = rows
                    .checked_add(u64::try_from(batch.num_rows()).map_err(|error| {
                        Error::new(ErrorKind::DataInvalid, "Compaction batch size overflow")
                            .with_source(error)
                    })?)
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
            let action = OverwriteFilesAction::for_rewrite(table, sequence_number)
                .remove_data_files(files)
                .add_data_files(outputs);
            let publication = action.new_state();
            state.prepared = Some((action, publication));
        }
        let (action, publication) = state.prepared.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Compaction publication was not prepared",
            )
        })?;
        action.commit(publication, table).await
    }

    async fn cleanup(self, _state: Self::State, _table: &Table, _status: CommitStatus) {}
}

fn row_count(files: &[DataFile]) -> Result<u64> {
    files.iter().try_fold(0_u64, |sum, file| {
        sum.checked_add(file.record_count())
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Compaction row count overflow"))
    })
}

#[cfg(test)]
mod tests;
