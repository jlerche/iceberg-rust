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

use std::sync::Arc;

use super::delete_fixture::{equality, equality_rows, missing, positions};
use super::{Fixture, live_entries, read_rows, row, write_rows};
use crate::catalog::MockCatalog;
use crate::io::CancellationToken;
use crate::spec::{DataFile, DataFileFormat, FormatVersion, Operation};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, RowDeltaAction, Transaction};
use crate::{Catalog, Error, ErrorKind, Result};

#[tokio::test]
async fn row_delta_replaces_composite_nullable_keys_then_compacts_without_resurrection()
-> Result<()> {
    let mut fixture = Fixture::new().await?;
    let old = vec![
        row(None, None),
        row(None, Some("a")),
        row(Some(1), Some("a")),
        row(Some(1), Some("b")),
        row(Some(2), Some("keep")),
    ];
    fixture.append(&old).await?;
    let data = write_rows(&fixture.table, &[
        row(Some(1), Some("new")),
        row(None, None),
    ])
    .await?;
    let deletes = equality_rows(
        &fixture.table,
        &[row(Some(1), Some("a")), row(None, None)],
        vec![1, 2],
        None,
    )
    .await?;
    let paths = data
        .iter()
        .chain(&deletes)
        .map(|file| file.file_path().to_string())
        .collect::<Vec<_>>();
    let action = Transaction::new(&fixture.table)
        .row_delta()
        .add_data_files(data)
        .add_delete_files(deletes);
    let prepared = action.prepare(&fixture.table).await?;
    let id = prepared.snapshot().snapshot_id();
    let inventory = prepared.artifacts();
    assert_eq!(
        (
            prepared.base_snapshot_id(),
            prepared.base_metadata_location(),
            prepared.snapshot().summary().operation.clone()
        ),
        (
            fixture.table.metadata().current_snapshot_id(),
            fixture.table.metadata_location(),
            Operation::Overwrite
        )
    );
    let before = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(before.metadata(), fixture.table.metadata());
    let fresh = fixture
        .catalog
        .update_table(prepared.try_into_table_commit()?)
        .await?;
    assert!(inventory.publication_protected());
    assert!(paths.iter().all(|path| !inventory.paths().contains(path)));
    let expected = vec![
        row(None, None),
        row(None, Some("a")),
        row(Some(1), Some("b")),
        row(Some(1), Some("new")),
        row(Some(2), Some("keep")),
    ];
    assert_eq!(
        (
            read_rows(&fresh, None).await?,
            read_rows(&fresh, fixture.table.metadata().current_snapshot_id()).await?
        ),
        (expected.clone(), old)
    );
    verify_sequences(&fresh, id).await?;
    let tx = Transaction::new(&fresh);
    let compacted = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&compacted, None).await?, expected);
    Ok(())
}

async fn verify_sequences(table: &Table, snapshot_id: i64) -> Result<()> {
    let sequences = live_entries(table)
        .await?
        .into_iter()
        .filter(|entry| entry.snapshot_id() == Some(snapshot_id))
        .map(|entry| (entry.sequence_number(), entry.file_sequence_number))
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![(Some(2), Some(2)), (Some(2), Some(2))]);
    Ok(())
}

#[tokio::test]
async fn row_delta_operations_match_data_delete_and_mixed_payloads() -> Result<()> {
    for (add_data, add_deletes, operation, expected) in [
        (true, false, Operation::Append, vec![
            row(Some(1), None),
            row(Some(2), None),
        ]),
        (false, true, Operation::Delete, vec![]),
        (true, true, Operation::Overwrite, vec![row(Some(1), None)]),
    ] {
        let mut fixture = Fixture::new().await?;
        fixture.append(&[row(Some(2), None)]).await?;
        let data = if add_data {
            write_rows(&fixture.table, &[row(Some(1), None)]).await?
        } else {
            vec![]
        };
        let deletes = if add_deletes {
            equality(&fixture.table, &[Some(2)], None).await?
        } else {
            vec![]
        };
        let tx = Transaction::new(&fixture.table);
        let action = tx
            .row_delta()
            .add_data_files(data)
            .add_delete_files(deletes);
        let inventory = action.artifacts();
        let fresh = action.apply(tx)?.commit(&fixture.catalog).await?;
        assert_eq!(
            (
                read_rows(&fresh, None).await?,
                fresh
                    .metadata()
                    .current_snapshot()
                    .ok_or_else(missing)?
                    .summary()
                    .operation
                    .clone()
            ),
            (expected, operation)
        );
        assert!(inventory.publication_protected());
    }
    Ok(())
}

async fn rejected(action: &RowDeltaAction, table: &Table, kind: ErrorKind) -> Result<()> {
    let before = action.artifacts().paths();
    let error = action.prepare(table).await.err().ok_or_else(missing)?;
    assert_eq!((error.kind(), action.artifacts().paths()), (kind, before));
    Ok(())
}

fn invalid_descriptors(file: &DataFile) -> Vec<DataFile> {
    let mut variants = vec![];
    let mut bad = file.clone();
    bad.file_path.clear();
    variants.push(bad);
    let mut bad = file.clone();
    bad.file_size_in_bytes = 0;
    variants.push(bad);
    let mut bad = file.clone();
    bad.file_size_in_bytes = u64::MAX;
    variants.push(bad);
    let mut bad = file.clone();
    bad.record_count = u64::MAX;
    variants.push(bad);
    let mut bad = file.clone();
    bad.partition_spec_id = 999;
    variants.push(bad);
    let mut bad = file.clone();
    bad.partition = crate::spec::Struct::from_iter([Some(crate::spec::Literal::int(1))]);
    variants.push(bad);
    let mut bad = file.clone();
    bad.referenced_data_file = Some("data.parquet".to_string());
    variants.push(bad);
    let mut bad = file.clone();
    bad.content_offset = Some(0);
    variants.push(bad);
    let mut bad = file.clone();
    bad.content_size_in_bytes = Some(10);
    variants.push(bad);
    variants
}

#[tokio::test]
async fn row_delta_rejects_invalid_descriptors_ids_content_and_formats_before_writing() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let data = write_rows(&fixture.table, &[row(Some(1), None)])
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    let delete = equality(&fixture.table, &[Some(1)], None)
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    let action = Transaction::new(&fixture.table).row_delta();
    rejected(&action, &fixture.table, ErrorKind::DataInvalid).await?;
    for file in invalid_descriptors(&data) {
        rejected(
            &action.clone().add_data_files([file]),
            &fixture.table,
            ErrorKind::DataInvalid,
        )
        .await?;
    }
    for ids in [None, Some(vec![]), Some(vec![1, 1]), Some(vec![999])] {
        let mut file = delete.clone();
        file.equality_ids = ids;
        rejected(
            &action.clone().add_delete_files([file]),
            &fixture.table,
            ErrorKind::DataInvalid,
        )
        .await?;
    }
    rejected(
        &action.clone().add_data_files([delete.clone()]),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await?;
    rejected(
        &action.clone().add_delete_files([data.clone()]),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await?;
    let mut format = delete;
    format.file_format = DataFileFormat::Puffin;
    rejected(
        &action.clone().add_delete_files([format]),
        &fixture.table,
        ErrorKind::FeatureUnsupported,
    )
    .await?;
    let positional = positions(&fixture.table, &[(data.file_path().to_string(), 0)]).await?;
    rejected(
        &action.add_delete_files(positional),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await
}

#[tokio::test]
async fn row_delta_rejects_duplicate_and_already_live_paths_across_contents() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    let data = write_rows(&fixture.table, &[row(Some(2), None)]).await?;
    let deletes = equality(&fixture.table, &[Some(1)], None).await?;
    let action = Transaction::new(&fixture.table).row_delta();
    rejected(
        &action
            .clone()
            .add_data_files(data.iter().chain(&data).cloned()),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await?;
    rejected(
        &action
            .clone()
            .add_delete_files(deletes.iter().chain(&deletes).cloned()),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await?;
    let mut collision = deletes.first().ok_or_else(missing)?.clone();
    collision.file_path = data.first().ok_or_else(missing)?.file_path().to_string();
    rejected(
        &action
            .clone()
            .add_data_files(data.clone())
            .add_delete_files([collision]),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await?;
    let old = live_entries(&fixture.table)
        .await?
        .first()
        .ok_or_else(missing)?
        .data_file()
        .clone();
    rejected(
        &action.clone().add_data_files([old]),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await?;
    fixture.table = action
        .add_delete_files(deletes.clone())
        .prepare(&fixture.table)
        .await?
        .publish(&fixture.catalog)
        .await?;
    let mut collision = data.first().ok_or_else(missing)?.clone();
    collision.file_path = deletes.first().ok_or_else(missing)?.file_path().to_string();
    rejected(
        &Transaction::new(&fixture.table)
            .row_delta()
            .add_data_files([collision]),
        &fixture.table,
        ErrorKind::DataInvalid,
    )
    .await
}

#[tokio::test]
async fn row_delta_rejects_concurrent_append_and_prepared_commit_races() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    let deletes = equality(&fixture.table, &[Some(1)], None).await?;
    let action = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files(deletes);
    let stale_transaction = action.clone().apply(Transaction::new(&fixture.table))?;
    let prepared = action.prepare(&fixture.table).await?;
    fixture.append(&[row(Some(1), Some("concurrent"))]).await?;
    let before = action.artifacts().paths();
    rejected(&action, &fixture.table, ErrorKind::PreconditionFailed).await?;
    let error = stale_transaction
        .commit(&fixture.catalog)
        .await
        .err()
        .ok_or_else(missing)?;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(action.artifacts().paths(), before);
    assert!(
        fixture
            .catalog
            .update_table(prepared.try_into_table_commit()?)
            .await
            .is_err()
    );
    assert_eq!(read_rows(&fixture.table, None).await?, vec![
        row(Some(1), None),
        row(Some(1), Some("concurrent"))
    ]);
    Ok(())
}

#[tokio::test]
async fn row_delta_rejects_unsupported_versions_and_readonly_tables() -> Result<()> {
    let fixture = Fixture::new().await?;
    let files = write_rows(&fixture.table, &[row(Some(1), None)]).await?;
    for version in [FormatVersion::V1, FormatVersion::V3] {
        let mut metadata = fixture.table.metadata().clone();
        metadata.format_version = version;
        let table = fixture.table.clone().with_metadata(Arc::new(metadata));
        rejected(
            &Transaction::new(&table)
                .row_delta()
                .add_data_files(files.clone()),
            &table,
            ErrorKind::FeatureUnsupported,
        )
        .await?;
    }
    let table = Table::builder()
        .metadata(fixture.table.metadata().clone())
        .identifier(fixture.table.identifier().clone())
        .file_io(fixture.table.file_io().clone())
        .readonly(true)
        .runtime(fixture.table.runtime().clone())
        .build()?;
    rejected(
        &Transaction::new(&table).row_delta().add_data_files(files),
        &table,
        ErrorKind::PreconditionFailed,
    )
    .await
}

#[tokio::test]
async fn row_delta_cancellation_preserves_caller_files_and_blocks_handoff() -> Result<()> {
    let fixture = Fixture::new().await?;
    let data = write_rows(&fixture.table, &[row(Some(1), None)]).await?;
    let token = CancellationToken::default();
    let action = Transaction::new(&fixture.table)
        .row_delta()
        .add_data_files(data.clone())
        .with_cancellation(token.clone());
    let prepared = action.prepare(&fixture.table).await?;
    token.cancel();
    let inventory = prepared.artifacts();
    let error = prepared.try_into_table_commit().err().ok_or_else(missing)?;
    assert_eq!(
        (error.kind(), inventory.publication_protected()),
        (ErrorKind::Cancelled, false)
    );
    rejected(&action, &fixture.table, ErrorKind::Cancelled).await?;
    for file in data {
        assert!(
            fixture
                .table
                .file_io()
                .new_input(file.file_path())?
                .exists()
                .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn row_delta_submitted_unknown_outcomes_are_not_retried_or_replaced_by_cancellation()
-> Result<()> {
    for prepared_path in [false, true] {
        let mut fixture = Fixture::new().await?;
        fixture.append(&[row(Some(1), Some("old"))]).await?;
        let data = write_rows(&fixture.table, &[row(Some(1), Some("new"))]).await?;
        let deletes = equality(&fixture.table, &[Some(1)], None).await?;
        let token = CancellationToken::default();
        let action = Transaction::new(&fixture.table)
            .row_delta()
            .add_data_files(data)
            .add_delete_files(deletes)
            .with_cancellation(token.clone());
        let inventory = action.artifacts();
        let base = fixture.table.clone();
        let backend = Arc::new(fixture.catalog);
        let mut catalog = MockCatalog::new();
        let loaded = base.clone();
        catalog.expect_load_table().times(1).returning(move |_| {
            let table = loaded.clone();
            Box::pin(async move { Ok(table) })
        });
        let stored = Arc::clone(&backend);
        catalog
            .expect_update_table()
            .times(1)
            .returning(move |commit| {
                let stored = Arc::clone(&stored);
                token.cancel();
                Box::pin(async move {
                    stored.update_table(commit).await?;
                    Err(
                        Error::new(ErrorKind::Unexpected, "Lost row-delta success response")
                            .with_retryable(true),
                    )
                })
            });
        let result = if prepared_path {
            action.prepare(&base).await?.publish(&catalog).await
        } else {
            action
                .apply(Transaction::new(&base))?
                .commit(&catalog)
                .await
        };
        let error = result.err().ok_or_else(missing)?;
        let fresh = backend.load_table(base.identifier()).await?;
        assert_eq!(
            (
                error.kind(),
                inventory.publication_protected(),
                fresh.metadata().snapshots().len(),
                read_rows(&fresh, None).await?
            ),
            (
                ErrorKind::Unexpected,
                true,
                base.metadata().snapshots().len() + 1,
                vec![row(Some(1), Some("new"))]
            )
        );
    }
    Ok(())
}

#[tokio::test]
async fn row_delta_metadata_failure_retains_caller_files_and_attempted_paths() -> Result<()> {
    let fixture = Fixture::new().await?;
    let data = write_rows(&fixture.table, &[row(Some(1), None)]).await?;
    let directory = tempfile::tempdir()?;
    let blocked = directory.path().join("blocked");
    std::fs::write(&blocked, b"not a directory")?;
    let metadata = fixture
        .table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(std::collections::HashMap::from([(
            "write.metadata.path".to_string(),
            blocked.to_string_lossy().into_owned(),
        )]))?
        .build()?
        .metadata;
    let table = fixture.table.clone().with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .row_delta()
        .add_data_files(data.clone());
    assert!(action.prepare(&table).await.is_err());
    assert_eq!(
        (
            action.artifacts().paths().len(),
            action.artifacts().publication_protected()
        ),
        (1, false)
    );
    for file in data {
        assert!(
            table
                .file_io()
                .new_input(file.file_path())?
                .exists()
                .await?
        );
    }
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(fresh.metadata(), fixture.table.metadata());
    Ok(())
}
