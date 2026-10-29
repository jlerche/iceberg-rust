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

use super::controlled::fixture_files;
use super::rebase::append_manifest;
use super::{Fixture, live_entries, read_rows, row};
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntryRef, ManifestFile, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    write_snapshot_commit,
};
use crate::transaction::{RewriteDataFilesAction, Transaction};
use crate::{Error, ErrorKind, Result};

fn missing() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Expected selected compaction metadata",
    )
}

async fn seed() -> Result<Fixture> {
    let mut fixture = Fixture::new().await?;
    for id in 0..8 {
        fixture.append(&[row(Some(id), Some("unchanged"))]).await?;
    }
    Ok(fixture)
}

fn selected(entries: &[ManifestEntryRef]) -> Result<Vec<DataFile>> {
    Ok(entries
        .get(..2)
        .ok_or_else(missing)?
        .iter()
        .map(|entry| entry.data_file().clone())
        .collect())
}

#[tokio::test]
async fn explicit_selection_retains_only_selected_descriptors_and_sequences() -> Result<()> {
    let fixture = seed().await?;
    let entries = live_entries(&fixture.table).await?;
    let mut selection = selected(&entries)?;
    selection.reverse();
    let paths = selection
        .iter()
        .map(|file| file.file_path().to_string())
        .collect::<HashSet<_>>();
    let action = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .select_data_files(selection.clone());
    let (retained, sequences) = action.inputs(&fixture.table).await?;
    assert_eq!(
        retained,
        selection.iter().rev().cloned().collect::<Vec<_>>()
    );
    assert_eq!(sequences.keys().cloned().collect::<HashSet<_>>(), paths);
    let bytes = selection.iter().map(DataFile::file_size_in_bytes).sum();
    let plan = action
        .with_plan_limits(2, bytes)?
        .plan()
        .await?
        .with_group_limits(2, bytes)?;
    assert_eq!(plan.input_files(), selection);
    let result = plan.execute().await?;
    let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    verify_survivors(&entries, &fresh, &result, &selection).await
}

async fn verify_survivors(
    entries: &[ManifestEntryRef],
    fresh: &Table,
    result: &crate::transaction::RewriteDataFilesResult,
    selection: &[DataFile],
) -> Result<()> {
    let survivors = |entries: &[ManifestEntryRef]| {
        entries
            .iter()
            .filter(|entry| !selection.contains(entry.data_file()))
            .map(|entry| {
                (
                    entry.data_file().clone(),
                    entry.snapshot_id(),
                    entry.sequence_number(),
                    entry.file_sequence_number,
                )
            })
            .collect::<Vec<_>>()
    };
    let live = live_entries(fresh).await?;
    let untouched = live
        .iter()
        .filter(|entry| !result.output_files().contains(entry.data_file()))
        .cloned()
        .collect::<Vec<_>>();
    let expected = (0..8)
        .map(|id| row(Some(id), Some("unchanged")))
        .collect::<Vec<_>>();
    let parent = fresh
        .metadata()
        .current_snapshot()
        .ok_or_else(missing)?
        .parent_snapshot_id()
        .ok_or_else(missing)?;
    assert_eq!(
        (
            survivors(&untouched),
            read_rows(fresh, None).await?,
            read_rows(fresh, Some(parent)).await?
        ),
        (survivors(entries), expected.clone(), expected)
    );
    Ok(())
}

#[tokio::test]
async fn empty_selection_retains_no_descriptors_or_sequences() -> Result<()> {
    let fixture = seed().await?;
    let action = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .select_data_files([]);
    let (files, sequences) = action.inputs(&fixture.table).await?;
    assert_eq!(
        (files, sequences),
        (vec![], std::collections::HashMap::new())
    );
    let result = action.plan().await?.execute().await?;
    assert!(!result.has_changes());
    assert!(result.artifacts().paths().is_empty());
    let all = Transaction::new(&fixture.table).rewrite_data_files();
    let (files, sequences) = all.inputs(&fixture.table).await?;
    assert_eq!((files.len(), sequences.len()), (8, 8));
    Ok(())
}

async fn rejects_without_outputs(
    action: &RewriteDataFilesAction,
    table: &Table,
    kind: ErrorKind,
) -> Result<()> {
    let data = fixture_files(table, "data")?;
    let metadata = fixture_files(table, "metadata")?;
    let error = action.plan().await.err().ok_or_else(missing)?;
    assert_eq!(
        (
            error.kind(),
            fixture_files(table, "data")?,
            fixture_files(table, "metadata")?
        ),
        (kind, data, metadata)
    );
    Ok(())
}

async fn with_manifests(table: &Table, manifests: Vec<ManifestFile>) -> Result<Table> {
    let id = generate_snapshot_id(table);
    let uuid = uuid::Uuid::now_v7();
    let mut commit = write_snapshot_commit(table, SnapshotCommitInput {
        snapshot_id: id,
        manifest_list_path: manifest_list_path(table, id, 0, uuid)?,
        operation: Operation::Append,
        properties: std::collections::HashMap::new(),
        added_files: &[],
        removed_files: &[],
        manifests,
        truncate_full_table: false,
    })
    .await?;
    Transaction::update_table_metadata(table.clone(), &commit.take_updates())
}

#[tokio::test]
async fn unselected_duplicate_paths_across_manifests_are_rejected() -> Result<()> {
    let fixture = seed().await?;
    let entries = live_entries(&fixture.table).await?;
    let selection = selected(&entries)?;
    let mut manifests = current_manifests(&fixture.table).await?;
    let duplicate = manifests.last().ok_or_else(missing)?.clone();
    manifests.push(duplicate);
    let table = with_manifests(&fixture.table, manifests).await?;
    let action = Transaction::new(&table)
        .rewrite_data_files()
        .select_data_files(selection);
    rejects_without_outputs(&action, &table, ErrorKind::DataInvalid).await?;
    let empty = Transaction::new(&table)
        .rewrite_data_files()
        .select_data_files([]);
    rejects_without_outputs(&empty, &table, ErrorKind::DataInvalid).await
}

#[tokio::test]
async fn unsupported_deletes_after_selected_inputs_are_rejected() -> Result<()> {
    let fixture = seed().await?;
    let entries = live_entries(&fixture.table).await?;
    let selection = selected(&entries)?;
    let mut delete = entries.last().ok_or_else(missing)?.as_ref().clone();
    delete.data_file.content = DataContentType::PositionDeletes;
    delete.data_file.file_path = "positions.puffin".to_string();
    delete.data_file.file_format = crate::spec::DataFileFormat::Puffin;
    let table = append_manifest(&fixture.table, vec![delete], ManifestContentType::Deletes).await?;
    let action = Transaction::new(&table)
        .rewrite_data_files()
        .select_data_files(selection);
    rejects_without_outputs(&action, &table, ErrorKind::FeatureUnsupported).await?;
    let empty = Transaction::new(&table)
        .rewrite_data_files()
        .select_data_files([]);
    rejects_without_outputs(&empty, &table, ErrorKind::FeatureUnsupported).await
}

#[tokio::test]
async fn invalid_unselected_data_sequences_are_rejected() -> Result<()> {
    let fixture = seed().await?;
    let entries = live_entries(&fixture.table).await?;
    let selection = selected(&entries)?;
    let mut modified = entries
        .iter()
        .map(|entry| entry.as_ref().clone())
        .collect::<Vec<_>>();
    modified.last_mut().ok_or_else(missing)?.sequence_number = Some(-1);
    let table = append_manifest(&fixture.table, modified, ManifestContentType::Data).await?;
    let action = Transaction::new(&table)
        .rewrite_data_files()
        .select_data_files(selection);
    rejects_without_outputs(&action, &table, ErrorKind::DataInvalid).await
}
