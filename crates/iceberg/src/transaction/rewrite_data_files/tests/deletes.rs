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

use super::delete_fixture::{equality, missing, positions, publish};
use super::{Fixture, live_entries, read_rows, row};
use crate::spec::{DataContentType, Operation};
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{ErrorKind, Result};

#[tokio::test]
async fn equality_materialization_preserves_newer_values_and_null_keys() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[
            row(Some(1), Some("old")),
            row(Some(2), Some("keep")),
            row(None, Some("old-null")),
        ])
        .await?;
    let deletes = equality(&fixture.table, &[Some(1), None], None).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    fixture
        .append(&[
            row(Some(1), Some("new")),
            row(None, Some("new-null")),
            row(Some(3), Some("keep")),
        ])
        .await?;
    fixture.append(&[row(Some(4), Some("four"))]).await?;
    let base = fixture.table.clone();
    let plan = Transaction::new(&base)
        .rewrite_data_files()
        .plan()
        .await?
        .with_group_limits(2, u64::MAX)?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| (group.input_files().len(), group.data_sequence_number()))
            .collect::<Vec<_>>(),
        vec![(2, 4), (1, 4)]
    );
    let result = plan.execute().await?;
    let prepared = result.prepare(&base).await?.ok_or_else(missing)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    let expected = vec![
        row(None, Some("new-null")),
        row(Some(1), Some("new")),
        row(Some(2), Some("keep")),
        row(Some(3), Some("keep")),
        row(Some(4), Some("four")),
    ];
    assert_eq!(
        (
            read_rows(&fresh, None).await?,
            read_rows(&fresh, base.metadata().current_snapshot_id()).await?
        ),
        (expected.clone(), expected)
    );
    let data = live_entries(&fresh)
        .await?
        .into_iter()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|entry| (entry.sequence_number(), entry.file_sequence_number))
        .collect::<Vec<_>>();
    assert_eq!(data, vec![(Some(4), Some(5)), (Some(4), Some(5))]);
    assert_eq!(
        super::super::deletes::inventory(&base).await?,
        super::super::deletes::inventory(&fresh).await?
    );
    Ok(())
}

#[tokio::test]
async fn shared_equality_deletes_remain_effective_for_unselected_files_and_future_deletes()
-> Result<()> {
    let mut fixture = Fixture::new().await?;
    for key in 2..5 {
        fixture
            .append(&[row(Some(1), Some("deleted")), row(Some(key), Some("keep"))])
            .await?;
    }
    let selected = live_entries(&fixture.table)
        .await?
        .into_iter()
        .take(2)
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let deletes = equality(&fixture.table, &[Some(1), Some(1)], None).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .rewrite_data_files()
        .select_data_files(selected)
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fixture.table, None).await?, vec![
        row(Some(2), Some("keep")),
        row(Some(3), Some("keep")),
        row(Some(4), Some("keep"))
    ]);
    let deletes = equality(&fixture.table, &[Some(2)], None).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    let expected = vec![row(Some(3), Some("keep")), row(Some(4), Some("keep"))];
    assert_eq!(read_rows(&fixture.table, None).await?, expected);
    let tx = Transaction::new(&fixture.table);
    let fresh = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, expected);
    Ok(())
}

#[tokio::test]
async fn shared_position_deletes_support_singleton_rewrites_without_resurrection() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(Some(1), None), row(Some(2), None), row(Some(3), None)])
        .await?;
    fixture
        .append(&[row(Some(4), None), row(Some(5), None)])
        .await?;
    let data = live_entries(&fixture.table)
        .await?
        .into_iter()
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let first = data.first().ok_or_else(missing)?.clone();
    let last = data.last().ok_or_else(missing)?;
    let deletes = positions(&fixture.table, &[
        (first.file_path().to_string(), 0),
        (last.file_path().to_string(), 0),
    ])
    .await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    let base = fixture.table.clone();
    let result = Transaction::new(&base)
        .rewrite_data_files()
        .select_data_files([first])
        .plan()
        .await?
        .execute()
        .await?;
    assert_eq!(
        (
            result.input_files().len(),
            result.output_files().len(),
            result.has_changes()
        ),
        (1, 1, true)
    );
    let fresh = result
        .prepare(&base)
        .await?
        .ok_or_else(missing)?
        .publish(&fixture.catalog)
        .await?;
    let expected = vec![row(Some(2), None), row(Some(3), None), row(Some(5), None)];
    assert_eq!(
        (
            read_rows(&fresh, None).await?,
            read_rows(&fresh, base.metadata().current_snapshot_id()).await?
        ),
        (expected.clone(), expected)
    );
    assert_eq!(
        super::super::deletes::inventory(&base).await?,
        super::super::deletes::inventory(&fresh).await?
    );
    Ok(())
}

#[tokio::test]
async fn fully_deleted_singleton_publishes_replace_without_output_data() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(Some(1), None), row(Some(2), None)])
        .await?;
    let deletes = equality(&fixture.table, &[Some(1), Some(2)], None).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    assert_eq!(
        (
            result.input_files().len(),
            result.output_files().len(),
            result.has_changes()
        ),
        (1, 0, true)
    );
    let fresh = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing)?
        .publish(&fixture.catalog)
        .await?;
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
        (vec![], Operation::Replace)
    );
    Ok(())
}

#[tokio::test]
async fn new_delete_during_execution_requires_replanning_and_retains_outputs() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let deletes = equality(&fixture.table, &[Some(1)], None).await?;
    publish(&mut fixture, &deletes, Operation::Append).await?;
    let before = result.artifacts().paths();
    let error = result
        .prepare(&fixture.table)
        .await
        .err()
        .ok_or_else(missing)?;
    assert_eq!(
        (error.kind(), result.artifacts().paths()),
        (ErrorKind::PreconditionFailed, before)
    );
    for file in result.output_files() {
        assert!(
            fixture
                .table
                .file_io()
                .new_input(file.file_path())?
                .exists()
                .await?
        );
    }
    assert_eq!(read_rows(&fixture.table, None).await?, vec![row(
        Some(2),
        None
    )]);
    Ok(())
}

#[tokio::test]
async fn append_repreparation_keeps_the_execution_delete_watermark() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), Some("old"))]).await?;
    let deletes = equality(&fixture.table, &[Some(1)], None).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    fixture.append(&[row(Some(1), Some("new"))]).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    fixture.append(&[row(Some(9), None)]).await?;
    let fresh = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing)?
        .publish(&fixture.catalog)
        .await?;
    let entries = live_entries(&fresh)
        .await?
        .into_iter()
        .filter(|entry| result.output_files().contains(entry.data_file()))
        .map(|entry| (entry.sequence_number(), entry.file_sequence_number))
        .collect::<Vec<_>>();
    assert_eq!(
        (read_rows(&fresh, None).await?, entries),
        (vec![row(Some(1), Some("new")), row(Some(9), None)], vec![(
            Some(3),
            Some(5)
        )])
    );
    Ok(())
}

#[tokio::test]
async fn removed_or_modified_live_deletes_require_replanning() -> Result<()> {
    use super::delete_fixture::replace_inventory;
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(Some(1), None), row(Some(2), None)])
        .await?;
    let files = equality(&fixture.table, &[Some(1)], None).await?;
    publish(&mut fixture, &files, Operation::Delete).await?;
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?
        .execute()
        .await?;
    let original = super::super::deletes::inventory(&fixture.table)
        .await?
        .into_values()
        .collect::<Vec<_>>();
    let mut sequence = original.clone();
    let entry = sequence.first_mut().ok_or_else(missing)?;
    entry.sequence_number = Some(3);
    entry.file_sequence_number = Some(3);
    let mut descriptor = original;
    descriptor
        .first_mut()
        .ok_or_else(missing)?
        .data_file
        .record_count += 1;
    let before = result.artifacts().paths();
    for inventory in [vec![], sequence, descriptor] {
        let table = replace_inventory(&fixture.table, inventory).await?;
        let error = result.prepare(&table).await.err().ok_or_else(missing)?;
        assert_eq!(
            (error.kind(), result.artifacts().paths()),
            (ErrorKind::PreconditionFailed, before.clone())
        );
    }
    Ok(())
}

#[tokio::test]
async fn unreadable_delete_file_fails_execution_without_publication() -> Result<()> {
    use crate::Catalog;
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(Some(1), None), row(Some(2), None)])
        .await?;
    let files = equality(&fixture.table, &[Some(1)], None).await?;
    publish(&mut fixture, &files, Operation::Delete).await?;
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?;
    let path = files.first().ok_or_else(missing)?.file_path();
    fixture.table.file_io().delete(path).await?;
    assert!(plan.execute().await.is_err());
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(fresh.metadata(), fixture.table.metadata());
    Ok(())
}

#[tokio::test]
async fn equality_and_position_deletes_materialize_together() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[
            row(Some(1), None),
            row(Some(2), Some("duplicate")),
            row(Some(2), Some("duplicate")),
            row(Some(3), Some("keep")),
        ])
        .await?;
    let path = live_entries(&fixture.table)
        .await?
        .first()
        .ok_or_else(missing)?
        .file_path()
        .to_string();
    let mut deletes = equality(&fixture.table, &[Some(1)], None).await?;
    deletes.extend(positions(&fixture.table, &[(path, 1)]).await?);
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    let expected = vec![row(Some(2), Some("duplicate")), row(Some(3), Some("keep"))];
    assert_eq!(read_rows(&fixture.table, None).await?, expected);
    let tx = Transaction::new(&fixture.table);
    let fresh = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, expected);
    Ok(())
}

#[tokio::test]
async fn same_commit_equality_deletes_do_not_remove_new_values() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), Some("old"))]).await?;
    let mut files = super::write_rows(&fixture.table, &[row(Some(1), Some("new"))]).await?;
    files.extend(equality(&fixture.table, &[Some(1)], None).await?);
    publish(&mut fixture, &files, Operation::Overwrite).await?;
    let expected = vec![row(Some(1), Some("new"))];
    assert_eq!(read_rows(&fixture.table, None).await?, expected);
    let tx = Transaction::new(&fixture.table);
    let fresh = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, expected);
    Ok(())
}
