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
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::transaction::Transaction;
use iceberg::{Catalog, ErrorKind, Result};

use super::cases::Case;
use super::deletes;
use super::faults::LostResponse;
use super::fixture::{Fixture, missing, read, retained, row, run};

#[test]
fn equality_row_delta_and_compaction_preserve_rows_on_rest_and_s3() -> Result<()> {
    run(async { Case::equality().await?.fixture.cleanup().await })
}

#[test]
fn position_row_delta_and_compaction_preserve_rows_on_rest_and_s3() -> Result<()> {
    run(async { Case::positions().await?.fixture.cleanup().await })
}

#[test]
fn rest_rejects_row_delta_requirements_after_matching_concurrent_append() -> Result<()> {
    run(async {
        let mut fixture = Fixture::new().await?;
        fixture.append(&[row(Some(1), Some("old"))]).await?;
        let deletes = deletes::equality(&fixture, Some(1)).await?;
        let caller_paths = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        let action = Transaction::new(&fixture.table)
            .row_delta()
            .add_delete_files(deletes);
        let prepared = action.prepare(&fixture.table).await?;
        let inventory = prepared.artifacts();
        fixture.append(&[row(Some(1), Some("concurrent"))]).await?;
        let error = fixture
            .catalog
            .update_table(prepared.try_into_table_commit()?)
            .await
            .err()
            .ok_or_else(missing)?;
        let fresh = fixture.fresh().await?;
        assert_eq!(
            (
                error.kind(),
                fresh.metadata(),
                read(&fresh, None).await?,
                inventory.publication_protected()
            ),
            (
                ErrorKind::CatalogCommitConflicts,
                fixture.table.metadata(),
                vec![row(Some(1), Some("concurrent")), row(Some(1), Some("old"))],
                true
            )
        );
        retained(&fresh, caller_paths).await?;
        retained(&fresh, inventory.paths()).await?;
        fixture.cleanup().await
    })
}

#[test]
fn row_delta_lost_rest_success_response_is_submitted_once_and_recovered_by_snapshot() -> Result<()>
{
    run(async {
        let mut fixture = Fixture::new().await?;
        fixture.append(&[row(Some(1), Some("old"))]).await?;
        let data = fixture.write(&[row(Some(1), Some("new"))]).await?;
        let deletes = deletes::equality(&fixture, Some(1)).await?;
        let caller_paths = data
            .iter()
            .chain(&deletes)
            .map(|file| file.file_path().to_string())
            .collect();
        let prepared = Transaction::new(&fixture.table)
            .row_delta()
            .add_data_files(data)
            .add_delete_files(deletes)
            .with_cancellation(fixture.cancellation.clone())
            .prepare(&fixture.table)
            .await?;
        let expected_snapshot = prepared.snapshot().clone();
        let inventory = prepared.artifacts();
        let catalog = LostResponse {
            catalog: Arc::clone(&fixture.catalog),
            updates: AtomicUsize::new(0),
            cancellation: fixture.cancellation.clone(),
        };
        let error = prepared.publish(&catalog).await.err().ok_or_else(missing)?;
        let fresh = fixture.fresh().await?;
        assert_eq!(
            (
                error.kind(),
                catalog.updates.load(Ordering::SeqCst),
                fresh.metadata().current_snapshot().map(AsRef::as_ref),
                fresh.metadata().snapshots().len(),
                read(&fresh, None).await?,
                inventory.publication_protected()
            ),
            (
                ErrorKind::Unexpected,
                1,
                Some(&expected_snapshot),
                fixture.table.metadata().snapshots().len() + 1,
                vec![row(Some(1), Some("new"))],
                true
            )
        );
        retained(&fresh, caller_paths).await?;
        retained(&fresh, inventory.paths()).await?;
        fixture.cleanup().await
    })
}
