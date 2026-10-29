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

use super::{Fixture, live_entries, read_rows, row};
use crate::spec::Operation;
use crate::transaction::Transaction;
use crate::{Catalog, Error, ErrorKind, Result};

fn missing_prepared() -> Error {
    Error::new(ErrorKind::Unexpected, "Expected a prepared rewrite")
}

fn fixture_files(table: &crate::table::Table, directory: &str) -> Result<Vec<std::path::PathBuf>> {
    let location = table
        .metadata()
        .location()
        .strip_prefix("file://")
        .ok_or_else(missing_prepared)?;
    let mut paths = std::fs::read_dir(std::path::Path::new(location).join(directory))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    Ok(paths)
}

#[tokio::test]
async fn caller_controls_execution_preparation_and_publication() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(None, None), row(Some(1), Some("same"))])
        .await?;
    fixture
        .append(&[row(Some(1), Some("same")), row(Some(2), None)])
        .await?;
    let base = &fixture.table;
    let metadata_files = fixture_files(base, "metadata")?;
    let plan = Transaction::new(base).rewrite_data_files().plan().await?;
    assert_eq!(
        (plan.input_files().len(), plan.base_snapshot_id()),
        (2, base.metadata().current_snapshot_id())
    );
    let result = plan.execute().await?;
    let after_execution = fixture.catalog.load_table(base.identifier()).await?;
    assert_eq!(fixture_files(base, "metadata")?, metadata_files);
    let output_paths = fixture_files(base, "data")?;
    assert_eq!(
        (
            after_execution.metadata(),
            result.input_files().len(),
            result.output_files().len()
        ),
        (base.metadata(), 2, 1)
    );
    let prepared = result
        .prepare(&after_execution)
        .await?
        .ok_or_else(missing_prepared)?;
    assert_eq!(fixture_files(base, "data")?, output_paths);
    let snapshot_id = prepared.snapshot().snapshot_id();
    let after_preparation = fixture.catalog.load_table(base.identifier()).await?;
    assert_eq!(
        (
            after_preparation.metadata(),
            prepared.base_metadata_location(),
            prepared.base_snapshot_id(),
            prepared.snapshot().summary().operation.clone()
        ),
        (
            base.metadata(),
            base.metadata_location(),
            base.metadata().current_snapshot_id(),
            Operation::Replace
        )
    );
    let committed = fixture
        .catalog
        .update_table(prepared.into_table_commit())
        .await?;
    let expected = vec![
        row(None, None),
        row(Some(1), Some("same")),
        row(Some(1), Some("same")),
        row(Some(2), None),
    ];
    assert_eq!(
        (
            committed.metadata().current_snapshot_id(),
            read_rows(&committed, None).await?,
            read_rows(&committed, base.metadata().current_snapshot_id()).await?
        ),
        (Some(snapshot_id), expected.clone(), expected)
    );
    Ok(())
}

#[tokio::test]
async fn selected_rewrite_preserves_unselected_files_and_selected_data_age() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), Some("untouched"))]).await?;
    fixture
        .append(&[row(None, None), row(Some(2), Some("duplicate"))])
        .await?;
    fixture
        .append(&[row(Some(2), Some("duplicate")), row(Some(3), None)])
        .await?;
    let base = &fixture.table;
    let entries = live_entries(base).await?;
    let selected = entries
        .iter()
        .filter(|entry| entry.sequence_number != Some(1))
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let untouched = entries
        .iter()
        .find(|entry| entry.sequence_number == Some(1))
        .ok_or_else(missing_prepared)?
        .data_file()
        .clone();
    let result = Transaction::new(base)
        .rewrite_data_files()
        .select_data_files(selected.clone())
        .plan()
        .await?
        .execute()
        .await?;
    assert_eq!(
        (
            result.input_files(),
            result
                .output_files()
                .iter()
                .map(crate::spec::DataFile::record_count)
                .sum::<u64>()
        ),
        (selected.as_slice(), 4)
    );
    let prepared = result.prepare(base).await?.ok_or_else(missing_prepared)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    let entries = live_entries(&fresh).await?;
    let unchanged = entries
        .iter()
        .find(|entry| entry.file_path() == untouched.file_path())
        .ok_or_else(missing_prepared)?
        .data_file()
        .clone();
    let sequence_numbers = entries
        .iter()
        .map(|entry| entry.sequence_number)
        .collect::<Vec<_>>();
    assert_eq!(
        (unchanged, sequence_numbers, read_rows(&fresh, None).await?),
        (untouched, vec![Some(1), Some(2)], vec![
            row(None, None),
            row(Some(1), Some("untouched")),
            row(Some(2), Some("duplicate")),
            row(Some(2), Some("duplicate")),
            row(Some(3), None)
        ])
    );
    Ok(())
}

#[tokio::test]
async fn prepared_commit_requirements_reject_concurrent_publication() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let prepared = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing_prepared)?;
    fixture.append(&[row(Some(3), None)]).await?;
    assert!(
        fixture
            .catalog
            .update_table(prepared.into_table_commit())
            .await
            .is_err()
    );
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (stored.metadata(), read_rows(&stored, None).await?),
        (fixture.table.metadata(), vec![
            row(Some(1), None),
            row(Some(2), None),
            row(Some(3), None)
        ])
    );
    for output in result.output_files() {
        assert!(
            stored
                .file_io()
                .new_input(output.file_path())?
                .exists()
                .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn planning_rejects_invalid_selections() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    let entries = live_entries(&fixture.table).await?;
    let file = entries
        .first()
        .ok_or_else(missing_prepared)?
        .data_file()
        .clone();
    let mut mismatch = file.clone();
    mismatch.record_count += 1;
    let mut missing = file.clone();
    missing.file_path = "missing.parquet".to_string();
    for files in [vec![file.clone(), file], vec![mismatch], vec![missing]] {
        let error = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .select_data_files(files)
            .plan()
            .await
            .err()
            .ok_or_else(missing_prepared)?;
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }
    Ok(())
}

#[tokio::test]
async fn caller_preparation_skips_noop_selection() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .select_data_files(vec![])
        .plan()
        .await?
        .execute()
        .await?;
    assert_eq!(
        (
            result.has_changes(),
            result.output_files().len(),
            result.prepare(&fixture.table).await?.is_none()
        ),
        (false, 0, true)
    );
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(stored.metadata(), fixture.table.metadata());
    Ok(())
}

#[tokio::test]
async fn executed_result_applies_without_rewriting_data() -> Result<()> {
    use crate::transaction::ApplyTransactionAction;

    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), Some("retained"))]).await?;
    let base = &fixture.table;
    let result = Transaction::new(base)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let inventory = result.artifacts();
    let outputs = result.output_files().to_vec();
    let data_paths = fixture_files(base, "data")?;
    let tx = Transaction::new(base);
    let committed = result.apply(tx)?.commit(&fixture.catalog).await?;
    assert!(inventory.publication_protected());
    let live = live_entries(&committed)
        .await?
        .iter()
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        (
            fixture_files(base, "data")?,
            live,
            read_rows(&committed, None).await?
        ),
        (data_paths, outputs, vec![
            row(Some(1), None),
            row(Some(2), Some("retained"))
        ])
    );
    Ok(())
}

#[tokio::test]
async fn prepared_publish_rejects_stale_base_without_submitting() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let prepared = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing_prepared)?;
    fixture.append(&[row(Some(3), None)]).await?;
    let error = prepared
        .publish(&fixture.catalog)
        .await
        .err()
        .ok_or_else(missing_prepared)?;
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (error.kind(), stored.metadata()),
        (ErrorKind::PreconditionFailed, fixture.table.metadata())
    );
    Ok(())
}

#[tokio::test]
async fn publication_does_not_retry_a_lost_success_response() -> Result<()> {
    use std::sync::Arc;

    use crate::catalog::MockCatalog;

    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let base = fixture.table.clone();
    let result = Transaction::new(&base)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let prepared = result.prepare(&base).await?.ok_or_else(missing_prepared)?;
    let snapshot_id = prepared.snapshot().snapshot_id();
    let stored = Arc::new(fixture.catalog);
    let mut catalog = MockCatalog::new();
    let loaded = base.clone();
    catalog.expect_load_table().times(1).returning(move |_| {
        let table = loaded.clone();
        Box::pin(async move { Ok(table) })
    });
    let backend = Arc::clone(&stored);
    catalog
        .expect_update_table()
        .times(1)
        .returning(move |commit| {
            let backend = Arc::clone(&backend);
            Box::pin(async move {
                backend.update_table(commit).await?;
                Err(Error::new(ErrorKind::Unexpected, "Lost catalog response").with_retryable(true))
            })
        });
    let publication_inventory = prepared.artifacts();
    let error = prepared
        .publish(&catalog)
        .await
        .err()
        .ok_or_else(missing_prepared)?;
    let fresh = stored.load_table(base.identifier()).await?;
    assert!(publication_inventory.publication_protected());
    assert_eq!(
        (
            error.kind(),
            fresh.metadata().current_snapshot_id(),
            fresh.metadata().snapshots().len(),
            read_rows(&fresh, None).await?
        ),
        (ErrorKind::Unexpected, Some(snapshot_id), 3, vec![
            row(Some(1), None),
            row(Some(2), None)
        ])
    );
    for output in result.output_files() {
        assert!(
            base.file_io()
                .new_input(output.file_path())?
                .exists()
                .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn group_limits_split_deterministically_and_preserve_rows() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    for id in 0..5 {
        fixture.append(&[row(Some(id), None)]).await?;
    }
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .with_group_limits(2, u64::MAX)?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| (group.input_files().len(), group.data_sequence_number()))
            .collect::<Vec<_>>(),
        vec![(2, 1), (2, 3), (1, 5)]
    );
    let result = plan.execute().await?;
    assert_eq!(
        (result.input_files().len(), result.output_files().len()),
        (4, 2)
    );
    let prepared = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing_prepared)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    assert_eq!(
        (
            read_rows(&fresh, None).await?,
            read_rows(&fresh, fixture.table.metadata().current_snapshot_id()).await?
        ),
        (
            (0..5).map(|id| row(Some(id), None)).collect::<Vec<_>>(),
            read_rows(&fixture.table, None).await?
        )
    );
    Ok(())
}

#[tokio::test]
async fn byte_limits_and_invalid_limits_are_checked_before_execution() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    for id in 0..3 {
        fixture.append(&[row(Some(id), None)]).await?;
    }
    let action = Transaction::new(&fixture.table).rewrite_data_files();
    let plan = action.plan().await?;
    let largest = plan
        .input_files()
        .iter()
        .map(crate::spec::DataFile::file_size_in_bytes)
        .max()
        .ok_or_else(missing_prepared)?;
    let limited = plan.with_group_limits(10, largest)?;
    assert!(limited.groups().iter().all(|group| {
        group
            .input_files()
            .iter()
            .map(crate::spec::DataFile::file_size_in_bytes)
            .sum::<u64>()
            <= largest
    }));
    for (files, bytes) in [(1, largest), (2, 0), (2, largest - 1)] {
        assert!(
            action
                .plan()
                .await?
                .with_group_limits(files, bytes)
                .is_err()
        );
    }
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(stored.metadata(), fixture.table.metadata());
    Ok(())
}

#[tokio::test]
async fn artifact_inventory_covers_data_metadata_and_publication_protection() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?;
    let inventory = plan.artifacts();
    assert!(inventory.paths().is_empty());
    let result = plan.execute().await?;
    let expected: std::collections::BTreeSet<_> = result
        .output_files()
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    assert_eq!(
        inventory
            .paths()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    let prepared = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing_prepared)?;
    assert!(
        inventory
            .paths()
            .contains(&prepared.snapshot().manifest_list().to_string())
    );
    assert!(!inventory.publication_protected());
    let _commit = prepared.into_table_commit();
    assert!(inventory.publication_protected());
    for input in result.input_files() {
        assert!(!inventory.paths().contains(&input.file_path().to_string()));
    }
    Ok(())
}

#[tokio::test]
async fn failed_execution_retains_allocated_output_inventory() -> Result<()> {
    use crate::transaction::ApplyTransactionAction;
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    let mut files = super::write_rows(&fixture.table, &[row(Some(2), None)]).await?;
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
    let plan = Transaction::new(&base).rewrite_data_files().plan().await?;
    let inventory = plan.artifacts();
    assert!(plan.execute().await.is_err());
    assert!(!inventory.paths().is_empty());
    assert!(!inventory.publication_protected());
    let fresh = fixture.catalog.load_table(base.identifier()).await?;
    assert_eq!(fresh.metadata(), base.metadata());
    Ok(())
}

#[tokio::test]
async fn failed_metadata_preparation_retains_attempted_manifest_paths() -> Result<()> {
    use std::collections::HashMap;
    use std::sync::Arc;
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let failed_storage = tempfile::TempDir::new()?;
    let blocked = failed_storage.path().join("blocked-metadata");
    std::fs::write(&blocked, b"not a directory")?;
    let prefix = blocked.to_string_lossy().into_owned();
    let metadata = fixture
        .table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            "write.metadata.path".to_string(),
            prefix.clone(),
        )]))?
        .build()?
        .metadata;
    let table = fixture.table.clone().with_metadata(Arc::new(metadata));
    let result = Transaction::new(&table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let inventory = result.artifacts();
    let data_paths = inventory.paths();
    assert!(result.prepare(&table).await.is_err());
    assert!(
        inventory
            .paths()
            .iter()
            .any(|path| path.starts_with(&prefix))
    );
    assert!(
        data_paths
            .iter()
            .all(|path| inventory.paths().contains(path))
    );
    assert!(!inventory.publication_protected());
    let stored = fixture.catalog.load_table(table.identifier()).await?;
    assert_eq!(stored.metadata(), fixture.table.metadata());
    Ok(())
}
