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

use super::{Fixture, read_rows, row};
use crate::catalog::MockCatalog;
use crate::io::CancellationToken;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, Error, ErrorKind, Result};

fn missing() -> Error {
    Error::new(ErrorKind::Unexpected, "Expected cancellation outcome")
}

async fn fixture() -> Result<Fixture> {
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), None)]).await?;
    fixture.append(&[row(Some(2), None)]).await?;
    Ok(fixture)
}

#[tokio::test]
async fn cancelled_planning_and_execution_allocate_no_outputs() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let action = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone());
    let plan = action.plan().await?;
    let inventory = plan.artifacts();
    token.cancel();
    let execution = plan.execute().await.err().ok_or_else(missing)?;
    let planning = action.plan().await.err().ok_or_else(missing)?;
    assert_eq!(
        (
            execution.kind(),
            planning.kind(),
            inventory.paths(),
            inventory.publication_protected()
        ),
        (ErrorKind::Cancelled, ErrorKind::Cancelled, vec![], false)
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_during_data_writing_retains_inventory_and_source_rows() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?;
    let inventory = plan.artifacts();
    inventory.cancel_on_registration(token);
    let error = plan.execute().await.err().ok_or_else(missing)?;
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (
            error.kind(),
            inventory.paths().len(),
            inventory.publication_protected(),
            stored.metadata(),
            read_rows(&stored, None).await?
        ),
        (
            ErrorKind::Cancelled,
            1,
            false,
            fixture.table.metadata(),
            vec![row(Some(1), None), row(Some(2), None)]
        )
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_during_metadata_preparation_retains_all_allocations() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?
        .execute()
        .await?;
    let inventory = result.artifacts();
    let data = inventory.paths();
    inventory.cancel_on_registration(token);
    let error = result
        .prepare(&fixture.table)
        .await
        .err()
        .ok_or_else(missing)?;
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (
            error.kind(),
            inventory.paths().len(),
            inventory.publication_protected(),
            stored.metadata()
        ),
        (
            ErrorKind::Cancelled,
            data.len() + 1,
            false,
            fixture.table.metadata()
        )
    );
    assert!(data.iter().all(|path| inventory.paths().contains(path)));
    Ok(())
}

#[tokio::test]
async fn cancellation_before_preparation_does_not_write_metadata() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?
        .execute()
        .await?;
    let inventory = result.artifacts();
    let paths = inventory.paths();
    token.cancel();
    let error = result
        .prepare(&fixture.table)
        .await
        .err()
        .ok_or_else(missing)?;
    assert_eq!(
        (
            error.kind(),
            inventory.paths(),
            inventory.publication_protected()
        ),
        (ErrorKind::Cancelled, paths, false)
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_after_base_load_prevents_catalog_submission() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?
        .execute()
        .await?;
    let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
    let inventory = prepared.artifacts();
    let mut catalog = MockCatalog::new();
    let base = fixture.table.clone();
    catalog.expect_load_table().times(1).returning(move |_| {
        token.cancel();
        let base = base.clone();
        Box::pin(async move { Ok(base) })
    });
    catalog.expect_update_table().times(0);
    let error = prepared.publish(&catalog).await.err().ok_or_else(missing)?;
    assert_eq!(
        (error.kind(), inventory.publication_protected()),
        (ErrorKind::Cancelled, false)
    );
    Ok(())
}

#[tokio::test]
async fn checked_handoff_rejects_cancellation_without_protecting_files() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?
        .execute()
        .await?;
    let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
    let inventory = prepared.artifacts();
    token.cancel();
    let error = prepared.try_into_table_commit().err().ok_or_else(missing)?;
    assert_eq!(
        (error.kind(), inventory.publication_protected()),
        (ErrorKind::Cancelled, false)
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_after_submission_preserves_success_and_unknown_outcomes() -> Result<()> {
    use std::sync::Arc;
    for lose_response in [false, true] {
        let fixture = fixture().await?;
        let token = CancellationToken::default();
        let result = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .with_cancellation(token.clone())
            .plan()
            .await?
            .execute()
            .await?;
        let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
        let snapshot_id = prepared.snapshot().snapshot_id();
        let inventory = prepared.artifacts();
        let base = fixture.table.clone();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().returning(move |_| {
            let base = base.clone();
            Box::pin(async move { Ok(base) })
        });
        let backend = Arc::new(fixture.catalog);
        let submitted = Arc::clone(&backend);
        catalog
            .expect_update_table()
            .times(1)
            .returning(move |commit| {
                let backend = Arc::clone(&submitted);
                token.cancel();
                Box::pin(publication_response(backend, commit, lose_response))
            });
        let outcome = prepared.publish(&catalog).await;
        assert_eq!(outcome.is_err(), lose_response);
        let stored = backend.load_table(fixture.table.identifier()).await?;
        assert_eq!(
            (
                stored.metadata().current_snapshot_id(),
                inventory.publication_protected()
            ),
            (Some(snapshot_id), true)
        );
    }
    Ok(())
}

#[derive(Clone)]
struct CancelBeforeSubmit(CancellationToken);

#[async_trait::async_trait]
impl crate::transaction::StatelessAction for CancelBeforeSubmit {
    async fn commit(
        &self,
        _table: &crate::table::Table,
    ) -> Result<crate::transaction::ActionCommit> {
        self.0.cancel();
        Ok(crate::transaction::ActionCommit::new(vec![], vec![]))
    }
}

#[tokio::test]
async fn transaction_rechecks_cancellation_after_all_actions() -> Result<()> {
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?
        .execute()
        .await?;
    let inventory = result.artifacts();
    let tx = result.apply(Transaction::new(&fixture.table))?;
    let tx = CancelBeforeSubmit(token).apply(tx)?;
    let error = tx
        .commit(&fixture.catalog)
        .await
        .err()
        .ok_or_else(missing)?;
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(
        (
            error.kind(),
            inventory.publication_protected(),
            stored.metadata()
        ),
        (ErrorKind::Cancelled, false, fixture.table.metadata())
    );
    Ok(())
}

#[tokio::test]
async fn noop_transactions_retain_their_cancellation_signal() -> Result<()> {
    for single_file in [false, true] {
        let mut fixture = Fixture::new().await?;
        if single_file {
            fixture.append(&[row(Some(1), None)]).await?;
        }
        let token = CancellationToken::default();
        let result = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .with_cancellation(token.clone())
            .plan()
            .await?
            .execute()
            .await?;
        let tx = result.apply(Transaction::new(&fixture.table))?;
        let tx = CancelBeforeSubmit(token).apply(tx)?;
        let error = tx
            .commit(&fixture.catalog)
            .await
            .err()
            .ok_or_else(missing)?;
        assert_eq!(error.kind(), ErrorKind::Cancelled);
    }
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_post_submission_error_when_cancellation_is_requested() -> Result<()>
{
    use std::sync::Arc;
    let fixture = fixture().await?;
    let token = CancellationToken::default();
    let base = fixture.table.clone();
    let result = Transaction::new(&base)
        .rewrite_data_files()
        .with_cancellation(token.clone())
        .plan()
        .await?
        .execute()
        .await?;
    let inventory = result.artifacts();
    let backend = Arc::new(fixture.catalog);
    let mut catalog = MockCatalog::new();
    let loaded = base.clone();
    catalog.expect_load_table().times(1).returning(move |_| {
        let table = loaded.clone();
        Box::pin(async move { Ok(table) })
    });
    let submitted = Arc::clone(&backend);
    catalog
        .expect_update_table()
        .times(1)
        .returning(move |commit| {
            let backend = Arc::clone(&submitted);
            token.cancel();
            Box::pin(async move {
                backend.update_table(commit).await?;
                Err(Error::new(ErrorKind::Unexpected, "Lost response").with_retryable(true))
            })
        });
    let error = result
        .apply(Transaction::new(&base))?
        .commit(&catalog)
        .await
        .err()
        .ok_or_else(missing)?;
    let stored = backend.load_table(base.identifier()).await?;
    assert_eq!(
        (
            error.kind(),
            error.retryable(),
            inventory.publication_protected(),
            stored.metadata().snapshots().len()
        ),
        (ErrorKind::Unexpected, false, true, 3)
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_mutations_preserve_main_reference_retention() -> Result<()> {
    use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
    use crate::{TableCommit, TableUpdate};
    let mut fixture = fixture().await?;
    let retention = SnapshotRetention::branch(Some(7), Some(86_400_000), Some(172_800_000));
    let snapshot_id = fixture
        .table
        .metadata()
        .current_snapshot_id()
        .ok_or_else(missing)?;
    let commit = TableCommit::builder()
        .ident(fixture.table.identifier().clone())
        .requirements(vec![])
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(snapshot_id, retention.clone()),
        }])
        .build();
    fixture.table = fixture.catalog.update_table(commit).await?;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(
        fixture
            .table
            .metadata()
            .refs
            .get(MAIN_BRANCH)
            .map(|reference| &reference.retention),
        Some(&retention)
    );
    fixture.append(&[row(Some(3), None)]).await?;
    assert_eq!(
        fixture
            .table
            .metadata()
            .refs
            .get(MAIN_BRANCH)
            .map(|reference| &reference.retention),
        Some(&retention)
    );
    Ok(())
}

async fn publication_response(
    backend: std::sync::Arc<crate::memory::MemoryCatalog>,
    commit: crate::TableCommit,
    lose_response: bool,
) -> Result<crate::table::Table> {
    let table = backend.update_table(commit).await?;
    if lose_response {
        return Err(Error::new(ErrorKind::Unexpected, "Lost response").with_retryable(true));
    }
    Ok(table)
}
