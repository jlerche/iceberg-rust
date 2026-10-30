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

use iceberg::spec::{DataContentType, ManifestEntryRef, Operation};
use iceberg::table::Table;
use iceberg::transaction::{
    ApplyTransactionAction, RewriteDataFilesResult, RowDeltaAction, Transaction,
};
use iceberg::{Catalog, Result};
use serde::Serialize;

use super::deletes;
use super::fixture::{Fixture, Row, live, missing, read, retained, row};

#[derive(Clone, Serialize)]
pub struct Check {
    pub label: String,
    pub snapshot_id: Option<i64>,
    pub operation: String,
    pub rows: Vec<Row>,
}

#[derive(Serialize)]
pub struct Expectations {
    pub head_snapshot_id: Option<i64>,
    pub namespace: String,
    pub table: String,
    pub checks: Vec<Check>,
}

pub struct Case {
    pub fixture: Fixture,
    pub checks: Vec<Check>,
}

impl Case {
    pub async fn equality() -> Result<Self> {
        let mut fixture = Fixture::new().await?;
        fixture
            .append(&[row(Some(1), Some("old")), row(Some(1), Some("old"))])
            .await?;
        fixture.append(&[row(None, Some("old-null"))]).await?;
        fixture.append(&[row(Some(2), Some("tombstone"))]).await?;
        fixture.append(&[row(Some(9), Some("untouched"))]).await?;
        let before = vec![
            row(None, Some("old-null")),
            row(Some(1), Some("old")),
            row(Some(1), Some("old")),
            row(Some(2), Some("tombstone")),
            row(Some(9), Some("untouched")),
        ];
        let mut data = fixture
            .write(&[row(Some(1), Some("new")), row(Some(1), Some("new"))])
            .await?;
        data.extend(fixture.write(&[row(None, Some("new-null"))]).await?);
        let mut deletes = vec![];
        for id in [None, Some(1), Some(2)] {
            deletes.extend(deletes::equality(&fixture, id).await?);
        }
        let action = Transaction::new(&fixture.table)
            .row_delta()
            .add_data_files(data)
            .add_delete_files(deletes);
        let mut case = Self::before(fixture, before)?;
        case.publish_delta(action).await?;
        case.after_delta(
            vec![
                row(None, Some("new-null")),
                row(Some(1), Some("new")),
                row(Some(1), Some("new")),
                row(Some(9), Some("untouched")),
            ],
            5,
        )
        .await?;
        Ok(case)
    }

    async fn publish_delta(&mut self, action: RowDeltaAction) -> Result<()> {
        let prepared = action.prepare(&self.fixture.table).await?;
        let id = prepared.snapshot().snapshot_id();
        let inventory = prepared.artifacts();
        let stored = self.fixture.fresh().await?;
        assert_eq!(stored.metadata(), self.fixture.table.metadata());
        self.fixture.table = self
            .fixture
            .catalog
            .update_table(prepared.try_into_table_commit()?)
            .await?;
        assert!(inventory.publication_protected());
        assert_eq!(
            self.fixture.table.metadata().current_snapshot_id(),
            Some(id)
        );
        Ok(())
    }

    pub async fn positions() -> Result<Self> {
        let mut fixture = Fixture::new().await?;
        fixture
            .append(&[
                row(Some(1), Some("one-a")),
                row(Some(1), Some("one-b")),
                row(Some(1), Some("one-c")),
            ])
            .await?;
        fixture
            .append(&[row(Some(1), Some("two-a")), row(Some(1), Some("two-b"))])
            .await?;
        fixture.append(&[row(Some(9), Some("untouched"))]).await?;
        let before = vec![
            row(Some(1), Some("one-a")),
            row(Some(1), Some("one-b")),
            row(Some(1), Some("one-c")),
            row(Some(1), Some("two-a")),
            row(Some(1), Some("two-b")),
            row(Some(9), Some("untouched")),
        ];
        let existing = live(&fixture.table).await?;
        let first = existing
            .iter()
            .find(|(_, entry)| entry.record_count() == 3)
            .ok_or_else(missing)?
            .1
            .file_path()
            .to_string();
        let second = existing
            .iter()
            .find(|(_, entry)| entry.record_count() == 2)
            .ok_or_else(missing)?
            .1
            .file_path()
            .to_string();
        let data = fixture
            .write(&[row(Some(1), Some("new-a")), row(Some(1), Some("new-b"))])
            .await?;
        let new = data.first().ok_or_else(missing)?.file_path().to_string();
        let deletes =
            deletes::positions(&fixture, Some(1), vec![(first, 1), (second, 0), (new, 1)]).await?;
        let mut case = Self::before(fixture, before)?;
        let tx = Transaction::new(&case.fixture.table);
        case.fixture.table = tx
            .row_delta()
            .add_data_files(data)
            .add_delete_files(deletes)
            .apply(tx)?
            .commit(case.fixture.catalog.as_ref())
            .await?;
        case.after_delta(
            vec![
                row(Some(1), Some("new-a")),
                row(Some(1), Some("one-a")),
                row(Some(1), Some("one-c")),
                row(Some(1), Some("two-b")),
                row(Some(9), Some("untouched")),
            ],
            2,
        )
        .await?;
        Ok(case)
    }

    fn before(fixture: Fixture, rows: Vec<Row>) -> Result<Self> {
        let snapshot_id = fixture
            .table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(missing)?;
        Ok(Self {
            fixture,
            checks: vec![Check {
                label: "before-delta".to_string(),
                snapshot_id: Some(snapshot_id),
                operation: "append".to_string(),
                rows,
            }],
        })
    }

    async fn after_delta(&mut self, rows: Vec<Row>, added_count: usize) -> Result<()> {
        let delta = self
            .fixture
            .table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(missing)?;
        self.checks.push(Check {
            label: "after-delta".to_string(),
            snapshot_id: Some(delta),
            operation: "overwrite".to_string(),
            rows: rows.clone(),
        });
        let sources = live(&self.fixture.table).await?;
        let delta_sequence = self.fixture.table.metadata().last_sequence_number();
        assert_added(&sources, delta, delta_sequence, added_count);
        let result = Transaction::new(&self.fixture.table)
            .rewrite_data_files()
            .plan()
            .await?
            .execute()
            .await?;
        let inventory = result.artifacts();
        let prepared = result
            .prepare(&self.fixture.table)
            .await?
            .ok_or_else(missing)?;
        self.fixture.table = prepared.publish(self.fixture.catalog.as_ref()).await?;
        let compacted = self
            .fixture
            .table
            .metadata()
            .current_snapshot()
            .ok_or_else(missing)?;
        assert_eq!(compacted.summary().operation, Operation::Replace);
        self.checks.push(Check {
            label: "after-compaction".to_string(),
            snapshot_id: Some(compacted.snapshot_id()),
            operation: "replace".to_string(),
            rows: rows.clone(),
        });
        self.checks.push(Check {
            label: "current".to_string(),
            snapshot_id: None,
            operation: "replace".to_string(),
            rows,
        });
        verify_rewrite(&self.fixture.table, &result, &sources, delta_sequence).await?;
        assert!(inventory.publication_protected());
        retained(&self.fixture.table, inventory.paths()).await?;
        self.verify().await
    }

    pub async fn verify(&self) -> Result<()> {
        for check in &self.checks {
            assert_eq!(
                read(&self.fixture.table, check.snapshot_id).await?,
                check.rows,
                "{}",
                check.label
            );
            let snapshot = check
                .snapshot_id
                .and_then(|id| self.fixture.table.metadata().snapshot_by_id(id))
                .or_else(|| self.fixture.table.metadata().current_snapshot())
                .ok_or_else(missing)?;
            assert_eq!(snapshot.summary().operation.as_str(), check.operation);
        }
        Ok(())
    }

    pub fn expectations(&self) -> Expectations {
        Expectations {
            head_snapshot_id: self.fixture.table.metadata().current_snapshot_id(),
            namespace: self.fixture.table.identifier().namespace().to_url_string(),
            table: self.fixture.table.identifier().name().to_string(),
            checks: self.checks.clone(),
        }
    }
}

fn assert_added(sources: &[(i32, ManifestEntryRef)], delta: i64, sequence: i64, count: usize) {
    let added = sources
        .iter()
        .filter(|(_, entry)| entry.snapshot_id() == Some(delta))
        .map(|(_, entry)| (entry.sequence_number(), entry.file_sequence_number))
        .collect::<Vec<_>>();
    assert_eq!(added, vec![(Some(sequence), Some(sequence)); count]);
}

async fn verify_rewrite(
    table: &Table,
    result: &RewriteDataFilesResult,
    sources: &[(i32, ManifestEntryRef)],
    delta_sequence: i64,
) -> Result<()> {
    let delete_entries = sources
        .iter()
        .filter(|(_, entry)| entry.content_type() != DataContentType::Data)
        .cloned()
        .collect::<Vec<_>>();
    let entries = live(table).await?;
    let deletes_after = entries
        .iter()
        .filter(|(_, entry)| entry.content_type() != DataContentType::Data)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(deletes_after, delete_entries);
    let outputs = entries
        .iter()
        .filter(|(_, entry)| result.output_files().contains(entry.data_file()))
        .map(|(_, entry)| (entry.sequence_number(), entry.file_sequence_number))
        .collect::<Vec<_>>();
    assert_eq!(outputs, vec![
        (
            Some(delta_sequence),
            Some(table.metadata().last_sequence_number())
        );
        result.output_files().len()
    ]);
    retained(
        table,
        sources
            .iter()
            .map(|(_, entry)| entry.file_path().to_string())
            .collect(),
    )
    .await
}
