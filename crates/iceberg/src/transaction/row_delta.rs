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

mod position_schema;
mod positions;
mod prepared;
mod validation;

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
pub use prepared::PreparedRowDelta;
use uuid::Uuid;

use super::action::StatelessAction;
use super::snapshot::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, new_manifest_writer, validate_files_not_live, write_snapshot_commit,
};
use super::{ActionCommit, PreparedRewriteDataFiles};
use crate::io::{ArtifactInventory, CancellationToken};
use crate::spec::{DataContentType, DataFile, ManifestContentType, Operation};
use crate::table::Table;
use crate::{Result, TableRequirement};

/// Publishes immutable caller-written data and row deletes in one snapshot.
///
/// Supports main on v2 tables with Parquet files. The planning base must remain
/// unchanged, including on transaction retry; concurrent appends or mutations
/// require a fresh action. This is whole-base conflict rejection, not a predicate
/// isolation policy. Use one snapshot-producing action per transaction.
/// Added rows and deletes inherit the same new data sequence,
/// so equality deletes affect older rows and preserve same-commit replacements.
/// Position deletes can explicitly target rows added in the same commit.
/// Callers must finish immutable file writes and supply accurate descriptors;
/// data/equality payloads are not reread. Position deletes are streamed and checked
/// against raw target-file Parquet row counts before metadata allocation. The
/// initial position mode requires two canonical columns, strictly ordered unique
/// `(path, position)` pairs, and matching target spec/partition. Targets may be
/// live data files or new data files in this delta. Files are never deleted.
///
/// ```no_run
/// use iceberg::spec::DataFile;
/// use iceberg::table::Table;
/// use iceberg::transaction::Transaction;
/// use iceberg::{Catalog, Result};
///
/// async fn publish(
///     base: &Table,
///     catalog: &dyn Catalog,
///     rows: Vec<DataFile>,
///     deletes: Vec<DataFile>,
/// ) -> Result<Table> {
///     let action = Transaction::new(base)
///         .row_delta()
///         .add_data_files(rows)
///         .add_delete_files(deletes);
///     let fresh = catalog.load_table(base.identifier()).await?;
///     let prepared = action.prepare(&fresh).await?;
///     catalog
///         .update_table(prepared.try_into_table_commit()?)
///         .await
/// }
/// ```
#[derive(Clone)]
pub struct RowDeltaAction {
    base: Table,
    data: Vec<DataFile>,
    deletes: Vec<DataFile>,
}

impl RowDeltaAction {
    pub(crate) fn new(table: &Table) -> Self {
        Self {
            base: table.clone().track_outputs(ArtifactInventory::default()),
            data: vec![],
            deletes: vec![],
        }
    }

    /// Supplies newly written data files to add atomically with the deletes.
    #[must_use]
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.data.extend(files);
        self
    }

    /// Supplies newly written equality or position-delete files.
    #[must_use]
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deletes.extend(files);
        self
    }

    /// Attaches a cooperative cancellation signal to preparation and publication.
    #[must_use]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.base = self.base.with_cancellation(token);
        self
    }

    /// Returns metadata outputs allocated by preparation attempts.
    ///
    /// Caller-written data and delete files are excluded; retain their paths
    /// separately. This inventory never authorizes physical deletion.
    #[must_use]
    pub fn artifacts(&self) -> ArtifactInventory {
        self.base.file_io().inventory().unwrap_or_default()
    }

    /// Writes snapshot metadata and returns a standard commit without publishing.
    ///
    /// # Errors
    /// Rejects a changed base, empty delta, invalid descriptors, unsupported
    /// formats, live path collisions, cancellation, input-reading errors, or
    /// metadata-writing errors.
    pub async fn prepare(&self, table: &Table) -> Result<PreparedRowDelta> {
        let table = self.tracked(table);
        let commit = self.build_commit(&table).await?;
        PreparedRewriteDataFiles::new(table, commit).map(PreparedRowDelta::new)
    }

    fn tracked(&self, table: &Table) -> Table {
        let mut table = table.clone().track_outputs(self.artifacts());
        if let Some(token) = self.base.file_io().cancellation() {
            table = table.with_cancellation(token);
        }
        table
    }

    async fn build_commit(&self, table: &Table) -> Result<ActionCommit> {
        self.base.file_io().check_cancellation()?;
        validation::validate(&self.base, table, &self.data, &self.deletes)?;
        let files = self
            .data
            .iter()
            .chain(&self.deletes)
            .cloned()
            .collect::<Vec<_>>();
        validate_files_not_live(table, &files).await?;
        positions::validate(table, &self.data, &self.deletes).await?;
        table.file_io().check_cancellation()?;
        let id = generate_snapshot_id(table);
        let uuid = Uuid::now_v7();
        let mut manifests = current_manifests(table).await?;
        manifests.extend(Self::manifests(table, id, uuid, &files).await?);
        let operation = if self.deletes.is_empty() {
            Operation::Append
        } else if self.data.is_empty() {
            Operation::Delete
        } else {
            Operation::Overwrite
        };
        let mut result = write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id: id,
            manifest_list_path: manifest_list_path(table, id, 0, uuid)?,
            operation,
            properties: HashMap::default(),
            added_files: &files,
            removed_files: &[],
            manifests,
            truncate_full_table: false,
        })
        .await?;
        table.file_io().check_cancellation()?;
        let updates = result.take_updates();
        let mut requirements = result.take_requirements();
        requirements.extend([
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table.metadata().current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: table.metadata().default_partition_spec_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: table.metadata().default_sort_order_id(),
            },
        ]);
        Ok(ActionCommit::new(updates, requirements)
            .with_artifacts(self.artifacts())
            .with_cancellation(self.base.file_io().cancellation()))
    }

    async fn manifests(
        table: &Table,
        id: i64,
        uuid: Uuid,
        files: &[DataFile],
    ) -> Result<Vec<crate::spec::ManifestFile>> {
        let mut groups: BTreeMap<_, Vec<&DataFile>> = BTreeMap::new();
        for file in files {
            groups
                .entry((
                    file.partition_spec_id,
                    file.content_type() == DataContentType::Data,
                ))
                .or_default()
                .push(file);
        }
        let mut manifests = vec![];
        for (index, ((spec_id, data), files)) in groups.into_iter().enumerate() {
            table.file_io().check_cancellation()?;
            let spec = table
                .metadata()
                .partition_spec_by_id(spec_id)
                .ok_or_else(validation::invalid)?
                .clone();
            let mut writer = new_manifest_writer(
                table,
                manifest_path(table, uuid, u64::try_from(index)?)?,
                id,
                if data {
                    ManifestContentType::Data
                } else {
                    ManifestContentType::Deletes
                },
                table.metadata().current_schema().clone(),
                spec,
            )?;
            for file in files {
                writer.add_file(file.clone(), -1)?;
            }
            manifests.push(writer.write_manifest_file().await?);
            table.file_io().check_cancellation()?;
        }
        Ok(manifests)
    }
}

#[async_trait]
impl StatelessAction for RowDeltaAction {
    async fn commit(&self, table: &Table) -> Result<ActionCommit> {
        self.build_commit(&self.tracked(table)).await
    }
}
