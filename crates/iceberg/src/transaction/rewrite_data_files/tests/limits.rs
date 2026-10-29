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

use super::controlled::fixture_files;
use super::{Fixture, live_entries, read_rows, row};
use crate::spec::DataFile;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, Error, ErrorKind, Result};

fn missing() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Expected compaction plan-limit result",
    )
}

async fn seed() -> Result<Fixture> {
    let mut fixture = Fixture::new().await?;
    for id in 0..3 {
        fixture.append(&[row(Some(id), None)]).await?;
    }
    Ok(fixture)
}

async fn files(fixture: &Fixture) -> Result<Vec<DataFile>> {
    Ok(live_entries(&fixture.table)
        .await?
        .iter()
        .map(|entry| entry.data_file().clone())
        .collect())
}

fn bytes(files: &[DataFile]) -> u64 {
    files.iter().map(DataFile::file_size_in_bytes).sum()
}

#[tokio::test]
async fn total_limits_accept_exact_boundaries_and_preserve_singleton_rows() -> Result<()> {
    let fixture = seed().await?;
    let inputs = files(&fixture).await?;
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_plan_limits(inputs.len(), bytes(&inputs))?
        .plan()
        .await?
        .with_group_limits(2, bytes(&inputs))?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| group.input_files().len())
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    let result = plan.execute().await?;
    assert_eq!(
        (result.input_files().len(), result.output_files().len()),
        (2, 1)
    );
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(fresh.metadata(), fixture.table.metadata());
    let prepared = result.prepare(&fresh).await?.ok_or_else(missing)?;
    let committed = prepared.publish(&fixture.catalog).await?;
    let expected = vec![row(Some(0), None), row(Some(1), None), row(Some(2), None)];
    assert_eq!(
        (
            read_rows(&committed, None).await?,
            read_rows(&committed, fixture.table.metadata().current_snapshot_id()).await?
        ),
        (expected.clone(), expected)
    );
    Ok(())
}

#[tokio::test]
async fn exceeded_total_limits_reject_without_writes_or_publication() -> Result<()> {
    let fixture = seed().await?;
    let inputs = files(&fixture).await?;
    let data = fixture_files(&fixture.table, "data")?;
    let metadata = fixture_files(&fixture.table, "metadata")?;
    for (count, size) in [(2, u64::MAX), (3, bytes(&inputs) - 1)] {
        let action = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .with_plan_limits(count, size)?;
        let error = action.plan().await.err().ok_or_else(missing)?;
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        let tx = Transaction::new(&fixture.table);
        let error = action
            .apply(tx)?
            .commit(&fixture.catalog)
            .await
            .err()
            .ok_or_else(missing)?;
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (
            fixture_files(&fresh, "data")?,
            fixture_files(&fresh, "metadata")?,
            fresh.metadata()
        ),
        (data, metadata, fixture.table.metadata())
    );
    for file in inputs {
        assert!(
            fresh
                .file_io()
                .new_input(file.file_path())?
                .exists()
                .await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn total_limits_charge_only_selected_files_independent_of_builder_order() -> Result<()> {
    let fixture = seed().await?;
    let mut selected = files(&fixture).await?;
    selected.truncate(2);
    selected.reverse();
    let action = Transaction::new(&fixture.table).rewrite_data_files();
    let before = action
        .clone()
        .with_plan_limits(2, bytes(&selected))?
        .select_data_files(selected.clone())
        .plan()
        .await?;
    let after = action
        .clone()
        .select_data_files(selected.clone())
        .with_plan_limits(2, bytes(&selected))?
        .plan()
        .await?;
    assert_eq!(
        (before.input_files(), after.input_files()),
        (selected.as_slice(), selected.as_slice())
    );
    assert!(before.artifacts().paths().is_empty());
    let tx = Transaction::new(&fixture.table);
    let fresh = action
        .clone()
        .with_plan_limits(2, bytes(&selected))?
        .select_data_files(selected)
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, vec![
        row(Some(0), None),
        row(Some(1), None),
        row(Some(2), None)
    ]);
    let empty = action
        .with_plan_limits(1, 1)?
        .select_data_files([])
        .plan()
        .await?
        .execute()
        .await?;
    assert_eq!(
        (
            empty.has_changes(),
            empty.prepare(&fixture.table).await?.is_none()
        ),
        (false, true)
    );
    Ok(())
}

#[tokio::test]
async fn zero_limits_reject_and_singleton_selection_is_admitted() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let action = Transaction::new(&fixture.table).rewrite_data_files();
    for (count, size) in [(0, 1), (1, 0), (0, 0)] {
        let error = action
            .clone()
            .with_plan_limits(count, size)
            .err()
            .ok_or_else(missing)?;
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }
    let empty = action
        .with_plan_limits(1, 1)?
        .plan()
        .await?
        .execute()
        .await?;
    assert!(!empty.has_changes());
    fixture.append(&[row(Some(1), None)]).await?;
    let inputs = files(&fixture).await?;
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_plan_limits(1, bytes(&inputs))?
        .plan()
        .await?;
    let inventory = plan.artifacts();
    let result = plan.execute().await?;
    assert_eq!(
        (
            result.has_changes(),
            result.prepare(&fixture.table).await?.is_none(),
            inventory.paths()
        ),
        (false, true, vec![])
    );
    Ok(())
}

#[tokio::test]
async fn total_source_byte_overflow_rejects_without_wrapping() -> Result<()> {
    let fixture = seed().await?;
    let mut inputs = files(&fixture).await?;
    inputs.truncate(2);
    inputs.first_mut().ok_or_else(missing)?.file_size_in_bytes = u64::MAX;
    inputs.last_mut().ok_or_else(missing)?.file_size_in_bytes = 1;
    let action = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_plan_limits(2, u64::MAX)?;
    let error = action
        .validate_plan_limits(&inputs)
        .err()
        .ok_or_else(missing)?;
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    Ok(())
}
