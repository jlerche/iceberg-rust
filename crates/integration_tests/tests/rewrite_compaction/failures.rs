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
use iceberg::{ErrorKind, Result};

use super::faults::{Fault, LostResponse};
use super::fixture::{Fixture, missing, read, retained};

#[test]
fn partial_s3_write_is_inventoried_without_publication() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let expected_rows = read(&fixture.table, None).await?;
        let plan = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .plan()
            .await?;
        let inventory = plan.artifacts();
        fixture.faults.arm(Fault::PartialData);
        let error = plan.execute().await.err().ok_or_else(missing)?;
        let paths = inventory.paths();
        assert_eq!(
            (error.kind(), paths.len(), inventory.publication_protected()),
            (ErrorKind::Unexpected, 1, false)
        );
        let partial = fixture
            .table
            .file_io()
            .new_input(paths.first().ok_or_else(missing)?)?
            .read()
            .await?;
        assert!(!partial.is_empty() && partial.len() <= 16);
        let fresh = fixture.fresh().await?;
        assert_eq!(
            (fresh.metadata(), read(&fresh, None).await?),
            (fixture.table.metadata(), expected_rows)
        );
        fixture.cleanup().await
    })
}

#[test]
fn manifest_close_failure_retains_real_s3_objects_without_publication() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let expected_rows = read(&fixture.table, None).await?;
        let result = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .plan()
            .await?
            .execute()
            .await?;
        let inventory = result.artifacts();
        let before = inventory.paths().len();
        fixture.faults.arm(Fault::MetadataClose);
        assert!(result.prepare(&fixture.table).await.is_err());
        assert_eq!(
            (inventory.paths().len(), inventory.publication_protected()),
            (before + 1, false)
        );
        retained(&fixture.table, inventory.paths()).await?;
        let fresh = fixture.fresh().await?;
        assert_eq!(
            (fresh.metadata(), read(&fresh, None).await?),
            (fixture.table.metadata(), expected_rows)
        );
        fixture.cleanup().await
    })
}

#[test]
fn cancellation_during_s3_writing_preserves_sources_and_inventory() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let expected_rows = read(&fixture.table, None).await?;
        let plan = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .with_cancellation(fixture.cancellation.clone())
            .plan()
            .await?;
        let inventory = plan.artifacts();
        fixture.faults.arm(Fault::CancelData);
        let error = plan.execute().await.err().ok_or_else(missing)?;
        assert_eq!(
            (
                error.kind(),
                inventory.paths().len(),
                inventory.publication_protected()
            ),
            (ErrorKind::Cancelled, 1, false)
        );
        let fresh = fixture.fresh().await?;
        assert_eq!(
            (fresh.metadata(), read(&fresh, None).await?),
            (fixture.table.metadata(), expected_rows)
        );
        fixture.cleanup().await
    })
}

#[test]
fn lost_rest_success_response_preserves_committed_snapshot_and_protected_outputs() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let expected_rows = read(&fixture.table, None).await?;
        let base = &fixture.table;
        let result = Transaction::new(base)
            .rewrite_data_files()
            .with_cancellation(fixture.cancellation.clone())
            .plan()
            .await?
            .execute()
            .await?;
        let prepared = result.prepare(base).await?.ok_or_else(missing)?;
        let snapshot_id = prepared.snapshot().snapshot_id();
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
                fresh.metadata().current_snapshot_id(),
                fresh.metadata().snapshots().len(),
                inventory.publication_protected(),
                read(&fresh, None).await?
            ),
            (
                ErrorKind::Unexpected,
                1,
                Some(snapshot_id),
                base.metadata().snapshots().len() + 1,
                true,
                expected_rows
            )
        );
        retained(&fresh, inventory.paths()).await?;
        fixture.cleanup().await
    })
}
