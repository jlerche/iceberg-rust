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

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema as ArrowSchema;
use futures::TryStreamExt;
use tempfile::TempDir;

use crate::cow_rewrite::writer::write_replacement_batches;
use crate::io::LocalFsStorageFactory;
use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use crate::spec::{DataFile, ManifestEntryRef, NestedField, Operation, PrimitiveType, Schema};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, Result, TableCreation};

type Row = (Option<i32>, Option<String>);

struct Fixture {
    catalog: MemoryCatalog,
    table: Table,
    _directory: TempDir,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let directory = TempDir::new()?;
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", directory.path().display()),
                )]),
            )
            .await?;
        let namespace = NamespaceIdent::new("compaction".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", PrimitiveType::Int).into(),
                NestedField::optional(2, "payload", PrimitiveType::String).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;
        Ok(Self {
            catalog,
            table,
            _directory: directory,
        })
    }

    async fn append(&mut self, rows: &[Row]) -> Result<()> {
        let files = write_rows(&self.table, rows).await?;
        let tx = Transaction::new(&self.table);
        self.table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)?
            .commit(&self.catalog)
            .await?;
        Ok(())
    }
}

fn row(id: Option<i32>, payload: Option<&str>) -> Row {
    (id, payload.map(str::to_string))
}

async fn write_rows(table: &Table, rows: &[Row]) -> Result<Vec<DataFile>> {
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::try_from(
            table.metadata().current_schema().as_ref(),
        )?),
        vec![
            Arc::new(rows.iter().map(|row| row.0).collect::<Int32Array>()),
            Arc::new(
                rows.iter()
                    .map(|row| row.1.as_deref())
                    .collect::<StringArray>(),
            ),
        ],
    )?;
    write_replacement_batches(
        table,
        table.metadata().current_schema().clone(),
        None,
        futures::stream::iter([Ok(batch)]),
    )
    .await
}

async fn read_rows(table: &Table, snapshot_id: Option<i64>) -> Result<Vec<Row>> {
    let mut scan = table.scan();
    if let Some(id) = snapshot_id {
        scan = scan.snapshot_id(id);
    }
    let mut stream = scan.build()?.to_arrow().await?;
    let mut rows = vec![];
    while let Some(batch) = stream.try_next().await? {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let strings = batch.column(1).as_string::<i32>();
        rows.extend((0..batch.num_rows()).map(|i| {
            (
                (!ids.is_null(i)).then(|| ids.value(i)),
                (!strings.is_null(i)).then(|| strings.value(i).to_string()),
            )
        }));
    }
    rows.sort();
    Ok(rows)
}

async fn live_entries(table: &Table) -> Result<Vec<ManifestEntryRef>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(vec![]);
    };
    let list = table.manifest_list_reader(snapshot).load().await?;
    let mut entries = vec![];
    for descriptor in list.entries() {
        let manifest = table.object_cache().get_manifest(descriptor).await?;
        entries.extend(manifest.entries().iter().filter(|e| e.is_alive()).cloned());
    }
    Ok(entries)
}

#[tokio::test]
async fn compaction_preserves_rows_history_and_data_age() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let first = vec![
        row(None, None),
        row(Some(1), Some("same")),
        row(Some(1), Some("same")),
    ];
    let second = vec![
        row(Some(2), None),
        row(Some(2), Some("")),
        row(Some(3), Some("value")),
    ];
    fixture.append(&first).await?;
    fixture.append(&second).await?;
    let base = fixture.table.clone();
    let sources = live_entries(&base).await?;
    let tx = Transaction::new(&base);
    let committed = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    let fresh = fixture.catalog.load_table(committed.identifier()).await?;
    let outputs = live_entries(&fresh).await?;
    let snapshot = fresh
        .metadata()
        .current_snapshot()
        .ok_or_else(|| crate::Error::new(ErrorKind::Unexpected, "Missing compaction snapshot"))?;
    let mut expected_rows = [first, second].concat();
    expected_rows.sort();
    let actual = (
        read_rows(&fresh, None).await?,
        read_rows(&fresh, base.metadata().current_snapshot_id()).await?,
        sources.len(),
        outputs.len(),
        snapshot.summary().operation.clone(),
        snapshot.parent_snapshot_id(),
        outputs
            .iter()
            .map(|entry| (entry.sequence_number, entry.file_sequence_number))
            .collect::<Vec<_>>(),
        snapshot
            .summary()
            .additional_properties
            .get("total-records")
            .cloned(),
    );
    let expected = (
        expected_rows.clone(),
        expected_rows,
        2,
        1,
        Operation::Replace,
        base.metadata().current_snapshot_id(),
        vec![(Some(1), Some(3))],
        Some("6".to_string()),
    );
    assert_eq!(actual, expected);
    for source in sources {
        assert!(
            base.file_io()
                .new_input(source.file_path())?
                .exists()
                .await?
        );
        assert!(
            outputs
                .iter()
                .all(|output| output.file_path() != source.file_path())
        );
    }
    Ok(())
}

#[tokio::test]
async fn compaction_skips_empty_and_single_file_tables() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    for rows in [vec![], vec![row(None, Some("retained"))]] {
        if !rows.is_empty() {
            fixture.append(&rows).await?;
        }
        let base = fixture.table.clone();
        let tx = Transaction::new(&base);
        let committed = tx
            .rewrite_data_files()
            .apply(tx)?
            .commit(&fixture.catalog)
            .await?;
        assert_eq!(
            (
                committed.metadata().current_snapshot_id(),
                read_rows(&committed, None).await?
            ),
            (base.metadata().current_snapshot_id(), rows)
        );
    }
    Ok(())
}

#[tokio::test]
async fn compaction_rejects_concurrent_append() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let tx = Transaction::new(&fixture.table);
    let stale = tx.rewrite_data_files().apply(tx)?;
    fixture.append(&[row(Some(3), Some("concurrent"))]).await?;
    let error =
        stale.commit(&fixture.catalog).await.err().ok_or_else(|| {
            crate::Error::new(ErrorKind::Unexpected, "Stale compaction succeeded")
        })?;
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (
            error.kind(),
            fresh.metadata(),
            read_rows(&fresh, None).await?
        ),
        (
            ErrorKind::PreconditionFailed,
            fixture.table.metadata(),
            vec![
                row(Some(1), None),
                row(Some(2), None),
                row(Some(3), Some("concurrent"))
            ]
        )
    );
    Ok(())
}

#[tokio::test]
async fn compaction_rejects_unsupported_tables_before_reading_files() -> Result<()> {
    use crate::transaction::TransactionAction;
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table, make_v3_minimal_table};

    for table in [
        make_v1_table(),
        make_v2_minimal_table(),
        make_v3_minimal_table(),
    ] {
        let action = Transaction::new(&table).rewrite_data_files();
        let mut state = action.new_state();
        let error = action
            .commit(&mut state, &table)
            .await
            .err()
            .ok_or_else(|| {
                crate::Error::new(ErrorKind::Unexpected, "Unsupported compaction succeeded")
            })?;
        assert_eq!(
            (error.kind(), state.prepared.is_none()),
            (ErrorKind::FeatureUnsupported, true)
        );
    }
    Ok(())
}

#[tokio::test]
async fn compaction_rejects_live_deletes_before_writing_output() -> Result<()> {
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, ManifestContentType, Struct,
    };
    use crate::transaction::TransactionAction;
    use crate::transaction::snapshot::{
        SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
        manifest_path, new_manifest_writer, write_snapshot_commit,
    };

    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    let table = &fixture.table;
    let id = generate_snapshot_id(table);
    let uuid = uuid::Uuid::now_v7();
    let mut writer = new_manifest_writer(
        table,
        manifest_path(table, uuid, 0)?,
        id,
        ManifestContentType::Deletes,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )?;
    let delete = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path("positions.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .map_err(|error| crate::Error::new(ErrorKind::DataInvalid, error.to_string()))?;
    writer.add_file(delete, -1)?;
    let mut manifests = current_manifests(table).await?;
    manifests.push(writer.write_manifest_file().await?);
    let mut commit = write_snapshot_commit(table, SnapshotCommitInput {
        snapshot_id: id,
        manifest_list_path: manifest_list_path(table, id, 0, uuid)?,
        operation: Operation::Delete,
        properties: HashMap::new(),
        added_files: &[],
        removed_files: &[],
        manifests,
        truncate_full_table: false,
    })
    .await?;
    let table = Transaction::update_table_metadata(table.clone(), &commit.take_updates())?;
    let action = Transaction::new(&table).rewrite_data_files();
    let mut state = action.new_state();
    let error = action
        .commit(&mut state, &table)
        .await
        .err()
        .ok_or_else(|| {
            crate::Error::new(ErrorKind::Unexpected, "Compaction with deletes succeeded")
        })?;
    assert_eq!(
        (error.kind(), state.prepared.is_none()),
        (ErrorKind::FeatureUnsupported, true)
    );
    Ok(())
}

#[tokio::test]
async fn compaction_rejects_source_row_count_mismatch_without_publication() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    let mut files = write_rows(&fixture.table, &[row(Some(2), None)]).await?;
    for file in &mut files {
        file.record_count += 1;
    }
    let tx = Transaction::new(&fixture.table);
    let base = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    let tx = Transaction::new(&base);
    let error = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await
        .err()
        .ok_or_else(|| crate::Error::new(ErrorKind::Unexpected, "Invalid compaction succeeded"))?;
    let fresh = fixture.catalog.load_table(base.identifier()).await?;
    assert_eq!(
        (error.kind(), fresh.metadata()),
        (ErrorKind::DataInvalid, base.metadata())
    );
    Ok(())
}

#[tokio::test]
async fn compaction_reuses_output_files_across_publication_attempts() -> Result<()> {
    use crate::transaction::TransactionAction;

    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let base = &fixture.table;
    let action = Transaction::new(base).rewrite_data_files();
    let mut state = action.new_state();
    let mut first = action.commit(&mut state, base).await?;
    let first = Transaction::update_table_metadata(base.clone(), &first.take_updates())?;
    let mut retry = action.commit(&mut state, base).await?;
    let retry = Transaction::update_table_metadata(base.clone(), &retry.take_updates())?;
    let first_files = live_entries(&first)
        .await?
        .iter()
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let retry_files = live_entries(&retry)
        .await?
        .iter()
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let stored = fixture.catalog.load_table(base.identifier()).await?;
    assert_eq!(
        (
            retry_files,
            stored.metadata(),
            read_rows(&retry, None).await?
        ),
        (first_files, base.metadata(), vec![
            row(Some(1), None),
            row(Some(2), None)
        ])
    );
    Ok(())
}

mod controlled;
