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
use std::sync::Arc;

use super::{Fixture, live_entries, read_rows, row};
use crate::spec::{Operation, TableMetadata};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, RewriteDataFilesResult, Transaction};
use crate::{Error, ErrorKind, Result};

fn missing() -> Error {
    Error::new(ErrorKind::Unexpected, "Expected a prepared rewrite")
}

async fn executed() -> Result<(Fixture, RewriteDataFilesResult)> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(None, None), row(Some(1), Some("duplicate"))])
        .await?;
    fixture.append(&[row(Some(1), Some("duplicate"))]).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    Ok((fixture, result))
}

#[tokio::test]
async fn append_descendants_preserve_rows_history_and_sequences_without_reexecution() -> Result<()>
{
    let (mut fixture, result) = executed().await?;
    let original = fixture.table.clone();
    let outputs = result.output_files().to_vec();
    fixture.append(&[row(Some(1), Some("duplicate"))]).await?;
    fixture.append(&[row(Some(9), None)]).await?;
    let appended = fixture.table.clone();
    let appended_entries = live_entries(&appended)
        .await?
        .into_iter()
        .filter(|entry| !result.input_files().contains(entry.data_file()))
        .collect::<Vec<_>>();
    let expected = vec![
        row(None, None),
        row(Some(1), Some("duplicate")),
        row(Some(1), Some("duplicate")),
        row(Some(1), Some("duplicate")),
        row(Some(9), None),
    ];
    let prepared = result.prepare(&appended).await?.ok_or_else(missing)?;
    assert_eq!(
        (
            prepared.base_snapshot_id(),
            prepared.base_metadata_location()
        ),
        (
            appended.metadata().current_snapshot_id(),
            appended.metadata_location()
        )
    );
    let fresh = prepared.publish(&fixture.catalog).await?;
    let entries = live_entries(&fresh).await?;
    let survivors = entries
        .iter()
        .filter(|entry| !outputs.contains(entry.data_file()))
        .cloned()
        .collect::<Vec<_>>();
    let rewritten = entries
        .iter()
        .filter(|entry| outputs.contains(entry.data_file()))
        .map(|entry| {
            (
                entry.data_file().clone(),
                entry.sequence_number(),
                entry.file_sequence_number,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        (
            survivors,
            rewritten,
            read_rows(&fresh, None).await?,
            read_rows(&fresh, appended.metadata().current_snapshot_id()).await?,
            read_rows(&fresh, original.metadata().current_snapshot_id()).await?
        ),
        (
            appended_entries,
            outputs
                .into_iter()
                .map(|file| (file, Some(1), Some(5)))
                .collect(),
            expected.clone(),
            expected,
            vec![
                row(None, None),
                row(Some(1), Some("duplicate")),
                row(Some(1), Some("duplicate"))
            ]
        )
    );
    assert_eq!(
        fresh
            .metadata()
            .current_snapshot()
            .ok_or_else(missing)?
            .summary()
            .operation,
        Operation::Replace
    );
    Ok(())
}

#[tokio::test]
async fn completed_result_applies_to_refreshed_transaction_after_append() -> Result<()> {
    let (mut fixture, result) = executed().await?;
    fixture.append(&[row(Some(3), None)]).await?;
    let expected = read_rows(&fixture.table, None).await?;
    let tx = Transaction::new(&fixture.table);
    let fresh = result.apply(tx)?.commit(&fixture.catalog).await?;
    assert_eq!(read_rows(&fresh, None).await?, expected);
    Ok(())
}

async fn rejects_without_metadata_outputs(
    result: &RewriteDataFilesResult,
    table: &Table,
) -> Result<()> {
    let before = result.artifacts().paths();
    let error = result.prepare(table).await.err().ok_or_else(missing)?;
    assert_eq!(
        (error.kind(), result.artifacts().paths()),
        (ErrorKind::PreconditionFailed, before)
    );
    for path in result.artifacts().paths() {
        assert!(table.file_io().new_input(path)?.exists().await?);
    }
    Ok(())
}

#[tokio::test]
async fn source_removal_and_intervening_replace_require_replanning() -> Result<()> {
    let (mut fixture, result) = executed().await?;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .overwrite_files()
        .remove_data_files(result.input_files().iter().take(1).cloned())
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    fixture.append(&[row(Some(4), None)]).await?;
    rejects_without_metadata_outputs(&result, &fixture.table).await?;
    let (mut fixture, result) = executed().await?;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    fixture.append(&[row(Some(4), None)]).await?;
    rejects_without_metadata_outputs(&result, &fixture.table).await
}

fn with_metadata(table: &Table, metadata: TableMetadata) -> Table {
    table.clone().with_metadata(Arc::new(metadata))
}

#[tokio::test]
async fn incompatible_configuration_and_missing_ancestry_require_replanning() -> Result<()> {
    let (mut fixture, result) = executed().await?;
    let base_id = fixture
        .table
        .metadata()
        .current_snapshot_id()
        .ok_or_else(missing)?;
    fixture.append(&[row(Some(4), None)]).await?;
    let table = &fixture.table;
    let changed = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            "write.parquet.compression-codec".to_string(),
            "gzip".to_string(),
        )]))?
        .build()?
        .metadata;
    rejects_without_metadata_outputs(&result, &with_metadata(table, changed)).await?;
    let expired = table
        .metadata()
        .clone()
        .into_builder(None)
        .remove_snapshots(&[base_id])
        .build()?
        .metadata;
    rejects_without_metadata_outputs(&result, &with_metadata(table, expired)).await
}

pub(super) async fn append_manifest(
    table: &Table,
    entries: Vec<crate::spec::ManifestEntry>,
    content: crate::spec::ManifestContentType,
) -> Result<Table> {
    use crate::transaction::snapshot::{
        SnapshotCommitInput, generate_snapshot_id, manifest_list_path, manifest_path,
        new_manifest_writer, write_snapshot_commit,
    };
    let id = generate_snapshot_id(table);
    let uuid = uuid::Uuid::now_v7();
    let mut writer = new_manifest_writer(
        table,
        manifest_path(table, uuid, 0)?,
        id,
        content,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )?;
    for entry in entries {
        writer.add_existing_entry(entry)?;
    }
    let mut manifests = if content == crate::spec::ManifestContentType::Deletes {
        crate::transaction::snapshot::current_manifests(table).await?
    } else {
        vec![]
    };
    manifests.push(writer.write_manifest_file().await?);
    let mut commit = write_snapshot_commit(table, SnapshotCommitInput {
        snapshot_id: id,
        manifest_list_path: manifest_list_path(table, id, 0, uuid)?,
        operation: Operation::Append,
        properties: HashMap::new(),
        added_files: &[],
        removed_files: &[],
        manifests,
        truncate_full_table: false,
    })
    .await?;
    Transaction::update_table_metadata(table.clone(), &commit.take_updates())
}

#[tokio::test]
async fn append_summary_cannot_hide_changed_unselected_files_or_sequences() -> Result<()> {
    let (mut fixture, _) = executed().await?;
    let selected = live_entries(&fixture.table)
        .await?
        .iter()
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    fixture.append(&[row(Some(8), None)]).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .select_data_files(selected)
        .plan()
        .await?
        .execute()
        .await?;
    let entries = live_entries(&fixture.table)
        .await?
        .iter()
        .map(|entry| entry.as_ref().clone())
        .collect::<Vec<_>>();
    let path = entries
        .iter()
        .find(|entry| !result.input_files().contains(entry.data_file()))
        .ok_or_else(missing)?
        .file_path()
        .to_string();
    for change in 0..3 {
        let changed = changed_entries(entries.clone(), &path, change)?;
        let table = append_manifest(
            &fixture.table,
            changed,
            crate::spec::ManifestContentType::Data,
        )
        .await?;
        rejects_without_metadata_outputs(&result, &table).await?;
    }
    Ok(())
}

#[tokio::test]
async fn append_with_live_deletes_is_rejected_before_metadata_writes() -> Result<()> {
    use crate::spec::{DataContentType, ManifestContentType, ManifestEntry};
    let (fixture, result) = executed().await?;
    let mut file = result.input_files().first().ok_or_else(missing)?.clone();
    file.content = DataContentType::PositionDeletes;
    file.file_path = "positions.parquet".to_string();
    let entry = ManifestEntry::builder()
        .data_file(file)
        .status(crate::spec::ManifestStatus::Existing)
        .snapshot_id(
            fixture
                .table
                .metadata()
                .current_snapshot_id()
                .ok_or_else(missing)?,
        )
        .sequence_number(2)
        .file_sequence_number(2)
        .build();
    let table = append_manifest(&fixture.table, vec![entry], ManifestContentType::Deletes).await?;
    let before = result.artifacts().paths();
    let error = result.prepare(&table).await.err().ok_or_else(missing)?;
    assert_eq!(
        (error.kind(), result.artifacts().paths()),
        (ErrorKind::FeatureUnsupported, before)
    );
    Ok(())
}

fn changed_entries(
    mut entries: Vec<crate::spec::ManifestEntry>,
    path: &str,
    change: u8,
) -> Result<Vec<crate::spec::ManifestEntry>> {
    if change == 0 {
        entries.retain(|entry| entry.file_path() != path);
        return Ok(entries);
    }
    let entry = entries
        .iter_mut()
        .find(|entry| entry.file_path() == path)
        .ok_or_else(missing)?;
    if change == 1 {
        entry.data_file.record_count += 1;
    } else {
        entry.sequence_number = Some(1);
    }
    Ok(entries)
}

#[tokio::test]
async fn evolved_schema_spec_sort_format_and_identity_require_replanning() -> Result<()> {
    use crate::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Transform,
    };
    let (mut fixture, result) = executed().await?;
    fixture.append(&[row(Some(4), None)]).await?;
    let table = &fixture.table;
    let mut variants = vec![];
    let mut metadata = table.metadata().clone();
    let mut fields = metadata.current_schema().as_struct().fields().to_vec();
    fields.push(NestedField::optional(3, "extra", PrimitiveType::String).into());
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()?;
    metadata.schemas.insert(1, Arc::new(schema));
    metadata.current_schema_id = 1;
    variants.push(metadata);
    let mut metadata = table.metadata().clone();
    let spec = PartitionSpec::builder(metadata.current_schema().clone())
        .with_spec_id(1)
        .add_partition_field("id", "id_partition", Transform::Identity)?
        .build()?;
    metadata.partition_specs.insert(1, Arc::new(spec));
    variants.push(metadata);
    let mut metadata = table.metadata().clone();
    metadata
        .sort_orders
        .insert(1, Arc::new(SortOrder::unsorted_order().with_order_id(1)));
    metadata.default_sort_order_id = 1;
    variants.push(metadata);
    let mut metadata = table.metadata().clone();
    metadata.format_version = FormatVersion::V3;
    variants.push(metadata);
    let mut metadata = table.metadata().clone();
    metadata.table_uuid = uuid::Uuid::now_v7();
    variants.push(metadata);
    let mut metadata = table.metadata().clone();
    metadata.location.push_str("/other");
    variants.push(metadata);
    for metadata in variants {
        rejects_without_metadata_outputs(&result, &with_metadata(table, metadata)).await?;
    }
    Ok(())
}
