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

use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use uuid::Uuid;

use super::action::CommitStatus;
use super::snapshot::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, new_manifest_writer, validate_files_not_live, validate_partition_value,
    write_snapshot_commit,
};
use super::{ActionCommit, TransactionAction};
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestStatus,
    Operation, TableMetadataRef,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result, TableRequirement};

/// Atomically replaces explicit live data files with caller-written files.
///
/// This is a logical overwrite, suitable for the old/new file sets returned by
/// [`crate::cow_rewrite::CowRewriteBuilder`]. It is not a content-preserving
/// compaction (`replace`) action. The caller owns replacement files and must
/// ensure they contain all intended rows, including unmatched rows of partially
/// updated files. Files are never physically deleted by this action.
///
/// The initial implementation supports main on v2 tables without live delete
/// files. It requires an unchanged planning base, including on retry, and does
/// not rebase a stale rewrite. Empty replacements are allowed for row deletion;
/// at least one source file must be removed. A no-op COW result should be skipped.
/// Create the transaction from the same table used to plan the COW rewrite.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use iceberg::cow_rewrite::{CowBatchRewriter, CowRewriteBuilder};
/// use iceberg::table::Table;
/// use iceberg::transaction::{ApplyTransactionAction, Transaction};
/// use iceberg::{Catalog, Result};
///
/// async fn rewrite(
///     table: &Table,
///     catalog: &dyn Catalog,
///     rewriter: Arc<dyn CowBatchRewriter>,
/// ) -> Result<Table> {
///     let result = CowRewriteBuilder::new(table)
///         .with_rewriter(rewriter)
///         .rewrite()
///         .await?;
///     if !result.has_changes() {
///         return Ok(table.clone());
///     }
///     let tx = Transaction::new(table);
///     tx.overwrite_files()
///         .remove_data_files(result.removed_data_files)
///         .add_data_files(result.added_data_files)
///         .apply(tx)?
///         .commit(catalog)
///         .await
/// }
/// ```
#[derive(Clone)]
pub struct OverwriteFilesAction {
    base: TableMetadataRef,
    removed: Vec<DataFile>,
    added: Vec<DataFile>,
}

impl OverwriteFilesAction {
    pub(crate) fn new(table: &Table) -> Self {
        Self {
            base: table.metadata_ref(),
            removed: vec![],
            added: vec![],
        }
    }

    /// Selects source files. Descriptors must match the live manifest entries.
    pub fn remove_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed.extend(files);
        self
    }

    /// Supplies new immutable files, already written by the caller.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added.extend(files);
        self
    }

    fn validate(&self, table: &Table) -> Result<()> {
        if table.metadata() != self.base.as_ref() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Overwrite planning base changed; replan the rewrite",
            ));
        }
        if self.base.format_version() != FormatVersion::V2 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Explicit file overwrite currently requires format v2",
            ));
        }
        if self.removed.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Overwrite requires source files",
            ));
        }
        let mut paths = HashSet::new();
        for file in self.removed.iter().chain(&self.added) {
            if file.content_type() != DataContentType::Data || !paths.insert(file.file_path()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Overwrite requires distinct data file paths, disjoint between inputs and outputs",
                ));
            }
            let spec = self
                .base
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Unknown overwrite partition spec")
                })?;
            validate_partition_value(
                file.partition(),
                &spec.partition_type(self.base.current_schema())?,
            )?;
        }
        Ok(())
    }
}

/// Retry state uses write-once paths and never deletes potentially published files.
pub(crate) struct OverwriteFilesState {
    commit_uuid: Uuid,
    attempt: u64,
    manifest_counter: u64,
}

#[async_trait]
impl TransactionAction for OverwriteFilesAction {
    type State = OverwriteFilesState;

    fn new_state(&self) -> Self::State {
        OverwriteFilesState {
            commit_uuid: Uuid::now_v7(),
            attempt: 0,
            manifest_counter: 0,
        }
    }

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        self.validate(table)?;
        validate_files_not_live(table, &self.added).await?;
        let requested: HashMap<_, _> = self.removed.iter().map(|f| (f.file_path(), f)).collect();
        let mut found = HashSet::new();
        let mut unchanged = vec![];
        let mut changed = vec![];
        // Resolve and validate every source before writing metadata. Entries from
        // the cache have inherited snapshot/data/file sequence numbers resolved.
        for descriptor in current_manifests(table).await? {
            let manifest = table.object_cache().get_manifest(&descriptor).await?;
            if descriptor.content == ManifestContentType::Deletes {
                if manifest.entries().iter().any(|e| e.is_alive()) {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Overwrite with live delete files is not yet supported",
                    ));
                }
                unchanged.push(descriptor);
                continue;
            }
            let mut touches = false;
            let mut entries = vec![];
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                if let Some(expected) = requested.get(entry.file_path()) {
                    if *expected != entry.data_file()
                        || !found.insert(entry.file_path().to_string())
                    {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Overwrite source descriptor mismatch or duplicate live source",
                        ));
                    }
                    touches = true;
                }
                entries.push(entry.as_ref().clone());
            }
            if touches {
                changed.push((descriptor.partition_spec_id, entries));
            } else {
                unchanged.push(descriptor);
            }
        }
        if found.len() != requested.len() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Overwrite source file is not live",
            ));
        }

        let snapshot_id = generate_snapshot_id(table);
        let mut manifests = unchanged;
        for (spec_id, entries) in changed {
            let path = manifest_path(table, state.commit_uuid, state.manifest_counter)?;
            state.manifest_counter += 1;
            let mut writer = new_manifest_writer(
                table,
                path,
                snapshot_id,
                ManifestContentType::Data,
                self.base.current_schema().clone(),
                self.base.partition_spec_by_id(spec_id).unwrap().clone(),
            )?;
            for entry in entries {
                if requested.contains_key(entry.file_path()) {
                    writer.add_delete_entry(entry)?;
                } else {
                    writer.add_existing_entry(entry)?;
                }
            }
            manifests.push(writer.write_manifest_file().await?);
        }
        let mut groups: BTreeMap<i32, Vec<&DataFile>> = BTreeMap::new();
        for file in &self.added {
            groups.entry(file.partition_spec_id).or_default().push(file);
        }
        for (spec_id, files) in groups {
            let path = manifest_path(table, state.commit_uuid, state.manifest_counter)?;
            state.manifest_counter += 1;
            let mut writer = new_manifest_writer(
                table,
                path,
                snapshot_id,
                ManifestContentType::Data,
                self.base.current_schema().clone(),
                self.base.partition_spec_by_id(spec_id).unwrap().clone(),
            )?;
            for file in files {
                writer.add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(file.clone())
                        .build(),
                )?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }
        let path = manifest_list_path(table, snapshot_id, state.attempt, state.commit_uuid)?;
        state.attempt += 1;
        let mut result = write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path: path,
            operation: if self.added.is_empty() {
                Operation::Delete
            } else {
                Operation::Overwrite
            },
            properties: HashMap::new(),
            added_files: &self.added,
            removed_files: &self.removed,
            manifests,
            truncate_full_table: false,
        })
        .await?;
        let updates = result.take_updates();
        let mut requirements = result.take_requirements();
        requirements.extend([
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.base.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: self.base.default_partition_spec_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: self.base.default_sort_order_id(),
            },
        ]);
        Ok(ActionCommit::new(updates, requirements))
    }

    async fn cleanup(self, _state: Self::State, _table: &Table, _status: CommitStatus) {
        // Preserve caller-owned outputs and all metadata until outcome-aware cleanup exists.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::Transaction;
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table, make_v3_minimal_table};

    fn file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    async fn seeded() -> (Table, Vec<DataFile>) {
        let table = make_v2_minimal_table();
        let files = vec![
            file(&table, "source-a.parquet"),
            file(&table, "source-b.parquet"),
        ];
        let action = Transaction::new(&table)
            .fast_append()
            .add_data_files(files.clone());
        let mut result = commit_with_fresh_state(action, &table).await.unwrap();
        let table = Transaction::update_table_metadata(table, &result.take_updates()).unwrap();
        (table, files)
    }

    #[tokio::test]
    async fn overwrite_preserves_survivor_identity_sequences_and_manifest_history() {
        let (table, files) = seeded().await;
        let parent = table.metadata().current_snapshot().unwrap();
        let old_list = table.manifest_list_reader(parent).load().await.unwrap();
        let old_manifest = table
            .object_cache()
            .get_manifest(&old_list.entries()[0])
            .await
            .unwrap();
        let old_entries = old_manifest.entries();
        let output = file(&table, "replacement.parquet");
        let action = Transaction::new(&table)
            .overwrite_files()
            .remove_data_files([files[0].clone()])
            .add_data_files([output.clone()]);
        let mut result = commit_with_fresh_state(action, &table).await.unwrap();
        for requirement in result.take_requirements() {
            requirement.check(Some(table.metadata())).unwrap();
        }
        let updated =
            Transaction::update_table_metadata(table.clone(), &result.take_updates()).unwrap();
        let snapshot = updated.metadata().current_snapshot().unwrap();
        let list = updated.manifest_list_reader(snapshot).load().await.unwrap();
        let mut entries = vec![];
        for descriptor in list.entries() {
            let manifest = updated
                .object_cache()
                .get_manifest(descriptor)
                .await
                .unwrap();
            entries.extend(manifest.entries().iter().cloned());
        }
        let deleted = entries
            .iter()
            .find(|e| e.file_path() == files[0].file_path())
            .unwrap();
        let survivor = entries
            .iter()
            .find(|e| e.file_path() == files[1].file_path())
            .unwrap();
        let added = entries
            .iter()
            .find(|e| e.file_path() == output.file_path())
            .unwrap();
        assert_eq!(deleted.status(), ManifestStatus::Deleted);
        assert_eq!(deleted.snapshot_id(), Some(snapshot.snapshot_id()));
        assert_eq!(deleted.sequence_number, old_entries[0].sequence_number);
        assert_eq!(
            deleted.file_sequence_number,
            old_entries[0].file_sequence_number
        );
        assert_eq!(survivor.status(), ManifestStatus::Existing);
        assert_eq!(survivor.snapshot_id(), old_entries[1].snapshot_id());
        assert_eq!(survivor.sequence_number, old_entries[1].sequence_number);
        assert_eq!(
            survivor.file_sequence_number,
            old_entries[1].file_sequence_number
        );
        assert_eq!(added.sequence_number(), Some(snapshot.sequence_number()));
        assert_eq!(added.file_sequence_number, Some(snapshot.sequence_number()));
        assert_eq!(
            snapshot.summary().additional_properties["total-data-files"],
            "2"
        );
        assert_eq!(old_manifest.entries().len(), 2);
        assert!(old_manifest.entries().iter().all(|e| e.is_alive()));
    }

    #[tokio::test]
    async fn overwrite_rejects_missing_mismatched_duplicate_and_live_output_files() {
        let (table, files) = seeded().await;
        let mut mismatch = files[0].clone();
        mismatch.record_count += 1;
        let actions = vec![
            Transaction::new(&table)
                .overwrite_files()
                .remove_data_files([file(&table, "missing")]),
            Transaction::new(&table)
                .overwrite_files()
                .remove_data_files([mismatch]),
            Transaction::new(&table)
                .overwrite_files()
                .remove_data_files([files[0].clone(), files[0].clone()]),
            Transaction::new(&table)
                .overwrite_files()
                .remove_data_files([files[0].clone()])
                .add_data_files([files[0].clone()]),
            Transaction::new(&table)
                .overwrite_files()
                .remove_data_files([files[0].clone()])
                .add_data_files([files[1].clone()]),
            Transaction::new(&table).overwrite_files(),
        ];
        for action in actions {
            assert!(commit_with_fresh_state(action, &table).await.is_err());
        }
    }

    #[tokio::test]
    async fn overwrite_rejects_unsupported_versions_before_manifest_io() {
        for table in [make_v1_table(), make_v3_minimal_table()] {
            let action = Transaction::new(&table)
                .overwrite_files()
                .remove_data_files([file(&table, "source")]);
            let err = commit_with_fresh_state(action, &table).await.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        }
    }
    #[tokio::test]
    async fn overwrite_rejects_live_delete_files_before_writing_metadata() {
        let (table, files) = seeded().await;
        let snapshot_id = generate_snapshot_id(&table);
        let mut writer = new_manifest_writer(
            &table,
            manifest_path(&table, Uuid::now_v7(), 0).unwrap(),
            snapshot_id,
            ManifestContentType::Deletes,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().clone(),
        )
        .unwrap();
        let mut delete = file(&table, "positions.parquet");
        delete.content = DataContentType::PositionDeletes;
        writer.add_file(delete, -1).unwrap();
        let mut manifests = current_manifests(&table).await.unwrap();
        manifests.push(writer.write_manifest_file().await.unwrap());
        let mut commit = write_snapshot_commit(&table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path: manifest_list_path(&table, snapshot_id, 0, Uuid::now_v7()).unwrap(),
            operation: Operation::Delete,
            properties: HashMap::new(),
            added_files: &[],
            removed_files: &[],
            manifests,
            truncate_full_table: false,
        })
        .await
        .unwrap();
        let table = Transaction::update_table_metadata(table, &commit.take_updates()).unwrap();
        let action = Transaction::new(&table)
            .overwrite_files()
            .remove_data_files([files[0].clone()]);
        let mut state = action.new_state();
        let err = action.commit(&mut state, &table).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        assert_eq!(state.manifest_counter, 0);
        assert_eq!(state.attempt, 0);
    }
}
