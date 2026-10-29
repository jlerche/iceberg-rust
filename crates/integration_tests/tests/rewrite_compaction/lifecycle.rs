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

use iceberg::spec::{Operation, PartitionSpec, Transform};
use iceberg::transaction::Transaction;
use iceberg::{Catalog, Result, TableRequirement, TableUpdate};
use iceberg_catalog_rest::REST_CATALOG_PROP_URI;
use iceberg_integration_tests::get_test_fixture;

use super::fixture::{Fixture, filtered, live, missing, read, retained, row};

async fn evolve(fixture: &mut Fixture) -> Result<()> {
    let table = &fixture.table;
    let spec = PartitionSpec::builder(table.metadata().current_schema().clone())
        .with_spec_id(1)
        .add_partition_field("id", "new_id", Transform::Identity)?
        .build()?
        .into_unbound();
    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: table.metadata().current_snapshot_id(),
        },
    ];
    let updates = vec![TableUpdate::AddSpec { spec }, TableUpdate::SetDefaultSpec {
        spec_id: 1,
    }];
    let endpoint = get_test_fixture()
        .catalog_config
        .get(REST_CATALOG_PROP_URI)
        .ok_or_else(missing)?;
    let namespace = table.identifier().namespace().to_url_string();
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{}",
        endpoint.trim_end_matches('/'),
        table.identifier().name()
    );
    reqwest::Client::new()
        .post(url)
        .json(&serde_json::json!({ "requirements": requirements, "updates": updates }))
        .send()
        .await?
        .error_for_status()?;
    fixture.table = fixture.fresh().await?;
    Ok(())
}

#[test]
fn rest_compaction_preserves_mixed_specs_rows_and_history() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        for _ in 0..2 {
            fixture.append(&[row(Some(1), Some("old"))]).await?;
        }
        evolve(&mut fixture).await?;
        for _ in 0..2 {
            fixture.append(&[row(Some(1), Some("new"))]).await?;
        }
        let base = &fixture.table;
        let sources = live(base).await?;
        let plan = Transaction::new(base)
            .rewrite_data_files()
            .plan()
            .await?
            .with_group_limits(2, u64::MAX)?;
        assert_eq!(
            plan.groups()
                .iter()
                .map(iceberg::transaction::RewriteDataFilesGroup::partition_spec_id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        let inventory = plan.artifacts();
        let result = plan.execute().await?;
        let after_execution = fixture.fresh().await?;
        assert_eq!(
            (after_execution.metadata(), inventory.paths().len()),
            (base.metadata(), 2)
        );
        retained(base, inventory.paths()).await?;
        let prepared = result
            .prepare(&after_execution)
            .await?
            .ok_or_else(missing)?;
        let snapshot_id = prepared.snapshot().snapshot_id();
        let after_preparation = fixture.fresh().await?;
        assert_eq!(
            (
                after_preparation.metadata(),
                inventory.publication_protected()
            ),
            (base.metadata(), false)
        );
        retained(base, inventory.paths()).await?;
        let committed = prepared.publish(fixture.catalog.as_ref()).await?;
        let fresh = fixture.fresh().await?;
        assert!(inventory.publication_protected());
        verify_mixed(&fresh, &committed, base, snapshot_id).await?;
        retained(
            base,
            sources
                .iter()
                .map(|(_, entry)| entry.file_path().to_string())
                .collect(),
        )
        .await?;
        fixture.cleanup().await
    })
}

async fn verify_mixed(
    fresh: &iceberg::table::Table,
    committed: &iceberg::table::Table,
    base: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<()> {
    let expected = vec![
        row(Some(1), Some("new")),
        row(Some(1), Some("new")),
        row(Some(1), Some("old")),
        row(Some(1), Some("old")),
    ];
    let ages: HashSet<_> = live(fresh)
        .await?
        .iter()
        .map(|(spec, entry)| (*spec, entry.sequence_number(), entry.file_sequence_number))
        .collect();
    let snapshot = fresh.metadata().current_snapshot().ok_or_else(missing)?;
    assert_eq!(
        (
            fresh.metadata(),
            read(fresh, None).await?,
            read(fresh, base.metadata().current_snapshot_id()).await?,
            filtered(fresh, 1).await?,
            ages,
            snapshot.summary().operation.clone(),
            snapshot.snapshot_id()
        ),
        (
            committed.metadata(),
            expected.clone(),
            expected.clone(),
            expected,
            HashSet::from([(0, Some(1), Some(5)), (1, Some(3), Some(5))]),
            Operation::Replace,
            snapshot_id
        )
    );
    Ok(())
}

#[test]
fn rest_compaction_preserves_null_partitions_and_duplicates() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let base = &fixture.table;
        let result = Transaction::new(base)
            .rewrite_data_files()
            .plan()
            .await?
            .execute()
            .await?;
        assert_eq!(
            (result.input_files().len(), result.output_files().len()),
            (4, 2)
        );
        let prepared = result.prepare(base).await?.ok_or_else(missing)?;
        let fresh = prepared.publish(fixture.catalog.as_ref()).await?;
        assert_eq!(
            (
                read(&fresh, None).await?,
                read(&fresh, base.metadata().current_snapshot_id()).await?
            ),
            (
                vec![
                    row(None, Some("duplicate")),
                    row(None, Some("duplicate")),
                    row(Some(1), Some("duplicate")),
                    row(Some(1), Some("duplicate"))
                ],
                read(base, None).await?
            )
        );
        fixture.cleanup().await
    })
}

#[test]
fn rest_catalog_allows_only_one_competing_prepared_rewrite() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let base = &fixture.table;
        let result = Transaction::new(base)
            .rewrite_data_files()
            .plan()
            .await?
            .execute()
            .await?;
        let first = result.prepare(base).await?.ok_or_else(missing)?;
        let second = result.prepare(base).await?.ok_or_else(missing)?;
        let first_id = first.snapshot().snapshot_id();
        let second_id = second.snapshot().snapshot_id();
        let (a, b) = tokio::join!(
            fixture.catalog.update_table(first.try_into_table_commit()?),
            fixture
                .catalog
                .update_table(second.try_into_table_commit()?)
        );
        assert_ne!(a.is_ok(), b.is_ok());
        let fresh = fixture.fresh().await?;
        let expected_id = if a.is_ok() { first_id } else { second_id };
        assert_eq!(
            (
                fresh.metadata().current_snapshot_id(),
                fresh.metadata().snapshots().len(),
                read(&fresh, None).await?
            ),
            (
                Some(expected_id),
                base.metadata().snapshots().len() + 1,
                read(base, None).await?
            )
        );
        retained(base, result.artifacts().paths()).await?;
        fixture.cleanup().await
    })
}

#[test]
fn rest_rejects_stale_rewrite_after_append() -> Result<()> {
    super::fixture::run(async {
        let mut fixture = Fixture::new().await?;
        fixture.seed().await?;
        let result = Transaction::new(&fixture.table)
            .rewrite_data_files()
            .plan()
            .await?
            .execute()
            .await?;
        let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
        fixture.append(&[row(Some(9), Some("concurrent"))]).await?;
        assert!(
            fixture
                .catalog
                .update_table(prepared.try_into_table_commit()?)
                .await
                .is_err()
        );
        let fresh = fixture.fresh().await?;
        assert_eq!(
            (fresh.metadata(), read(&fresh, None).await?),
            (fixture.table.metadata(), read(&fixture.table, None).await?)
        );
        retained(&fresh, result.artifacts().paths()).await?;
        fixture.cleanup().await
    })
}
