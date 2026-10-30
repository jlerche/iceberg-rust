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

use std::collections::{HashMap, HashSet};

use super::{Fixture, Row, live_entries, read_rows, read_scan_rows, row, write_partition_rows};
use crate::spec::{
    Literal, Operation, PartitionKey, PartitionSpec, Struct, TableProperties, Transform,
};
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, Error, ErrorKind, Result, TableCommit, TableCreation, TableUpdate};

fn missing() -> Error {
    Error::new(ErrorKind::Unexpected, "Missing partition rewrite metadata")
}

fn partition(value: Option<i32>) -> Struct {
    Struct::from_iter([value.map(Literal::int)])
}

async fn fixture() -> Result<Fixture> {
    fixture_with_transform(Transform::Identity).await
}

async fn fixture_with_transform(transform: Transform) -> Result<Fixture> {
    let mut fixture = Fixture::new().await?;
    let schema = fixture.table.metadata().current_schema().clone();
    let spec = PartitionSpec::builder(schema.clone())
        .add_partition_field("id", "old_id", transform)?
        .build()?;
    fixture.table = fixture
        .catalog
        .create_table(
            fixture.table.identifier().namespace(),
            TableCreation::builder()
                .name("partitioned".to_string())
                .schema(schema.as_ref().clone())
                .partition_spec(spec.into_unbound())
                .properties(HashMap::from([(
                    TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT.to_string(),
                    "10".to_string(),
                )]))
                .build(),
        )
        .await?;
    Ok(fixture)
}

async fn append(fixture: &mut Fixture, value: Option<i32>, rows: &[Row]) -> Result<()> {
    append_key(fixture, partition(value), rows).await
}

async fn append_key(fixture: &mut Fixture, value: Struct, rows: &[Row]) -> Result<()> {
    let metadata = fixture.table.metadata();
    let key = PartitionKey::new(
        metadata.default_partition_spec().as_ref().clone(),
        metadata.current_schema().clone(),
        value,
    );
    let files = write_partition_rows(&fixture.table, rows, Some(key)).await?;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    Ok(())
}

async fn seed_groups() -> Result<Fixture> {
    let mut fixture = fixture().await?;
    for value in [Some(1), Some(2), None] {
        for _ in 0..2 {
            append(&mut fixture, value, &[row(value, Some("duplicate"))]).await?;
        }
    }
    append(&mut fixture, Some(3), &[row(Some(3), Some("untouched"))]).await?;
    Ok(fixture)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FileState {
    spec: i32,
    partition: Struct,
    records: u64,
    data_sequence: Option<i64>,
    file_sequence: Option<i64>,
}

async fn file_states(table: &crate::table::Table) -> Result<HashSet<FileState>> {
    Ok(live_entries(table)
        .await?
        .iter()
        .map(|entry| FileState {
            spec: entry.data_file().partition_spec_id,
            partition: entry.data_file().partition().clone(),
            records: entry.record_count(),
            data_sequence: entry.sequence_number,
            file_sequence: entry.file_sequence_number,
        })
        .collect())
}

fn state(spec: i32, value: Option<i32>, records: u64, sequences: (i64, i64)) -> FileState {
    FileState {
        spec,
        partition: partition(value),
        records,
        data_sequence: Some(sequences.0),
        file_sequence: Some(sequences.1),
    }
}

#[tokio::test]
async fn compaction_preserves_partition_groups_nulls_data_age_and_history() -> Result<()> {
    let fixture = seed_groups().await?;
    let base = &fixture.table;
    let plan = Transaction::new(base).rewrite_data_files().plan().await?;
    let groups = plan
        .groups()
        .iter()
        .map(|group| {
            (
                group.partition_spec_id(),
                group.partition().clone(),
                group.input_files().len(),
                group.data_sequence_number(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(groups, vec![
        (0, partition(Some(1)), 2, 1),
        (0, partition(Some(2)), 2, 3),
        (0, partition(None), 2, 5),
        (0, partition(Some(3)), 1, 7)
    ]);
    let result = plan.execute().await?;
    assert_eq!(
        (result.input_files().len(), result.output_files().len()),
        (6, 3)
    );
    let prepared = result.prepare(base).await?.ok_or_else(missing)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    let expected_rows = vec![
        row(None, Some("duplicate")),
        row(None, Some("duplicate")),
        row(Some(1), Some("duplicate")),
        row(Some(1), Some("duplicate")),
        row(Some(2), Some("duplicate")),
        row(Some(2), Some("duplicate")),
        row(Some(3), Some("untouched")),
    ];
    let snapshot = fresh.metadata().current_snapshot().ok_or_else(missing)?;
    assert_eq!(
        (
            file_states(&fresh).await?,
            read_rows(&fresh, None).await?,
            read_rows(&fresh, base.metadata().current_snapshot_id()).await?,
            snapshot.summary().operation.clone(),
            snapshot
                .summary()
                .additional_properties
                .get("total-data-files")
                .cloned()
        ),
        (
            HashSet::from([
                state(0, Some(1), 2, (1, 8)),
                state(0, Some(2), 2, (3, 8)),
                state(0, None, 2, (5, 8)),
                state(0, Some(3), 1, (7, 7))
            ]),
            expected_rows.clone(),
            expected_rows,
            Operation::Replace,
            Some("4".to_string())
        )
    );
    Ok(())
}

#[tokio::test]
async fn selected_partition_rewrite_preserves_unselected_descriptors() -> Result<()> {
    let fixture = seed_groups().await?;
    let entries = live_entries(&fixture.table).await?;
    let selected = entries
        .iter()
        .filter(|entry| entry.data_file().partition() == &partition(Some(2)))
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let untouched: HashMap<_, _> = entries
        .iter()
        .filter(|entry| entry.data_file().partition() != &partition(Some(2)))
        .map(|entry| (entry.file_path().to_string(), entry.data_file().clone()))
        .collect();
    let result = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .select_data_files(selected)
        .plan()
        .await?
        .execute()
        .await?;
    let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    let retained: HashMap<_, _> = live_entries(&fresh)
        .await?
        .iter()
        .filter(|entry| untouched.contains_key(entry.file_path()))
        .map(|entry| (entry.file_path().to_string(), entry.data_file().clone()))
        .collect();
    assert_eq!(
        (
            retained,
            result.input_files().len(),
            result.output_files().len(),
            read_rows(&fresh, None).await?
        ),
        (untouched, 2, 1, read_rows(&fixture.table, None).await?)
    );
    Ok(())
}

#[tokio::test]
async fn singleton_partition_groups_are_noops() -> Result<()> {
    let mut fixture = fixture().await?;
    append(&mut fixture, Some(1), &[row(Some(1), None)]).await?;
    append(&mut fixture, None, &[row(None, None)]).await?;
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?;
    assert_eq!(plan.groups().len(), 2);
    let result = plan.execute().await?;
    assert_eq!(
        (
            result.has_changes(),
            result.input_files().len(),
            result.output_files().len(),
            result.prepare(&fixture.table).await?.is_none()
        ),
        (false, 0, 0, true)
    );
    let stored = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(stored.metadata(), fixture.table.metadata());
    Ok(())
}

async fn evolve(fixture: &mut Fixture) -> Result<()> {
    let spec = PartitionSpec::builder(fixture.table.metadata().current_schema().clone())
        .with_spec_id(1)
        .add_partition_field("id", "new_id", Transform::Identity)?
        .build()?
        .into_unbound();
    let commit = TableCommit::builder()
        .ident(fixture.table.identifier().clone())
        .requirements(vec![])
        .updates(vec![
            TableUpdate::AddSpec { spec },
            TableUpdate::SetDefaultSpec { spec_id: 1 },
        ])
        .build();
    fixture.table = fixture.catalog.update_table(commit).await?;
    Ok(())
}

#[tokio::test]
async fn spec_evolution_keeps_equal_partition_tuples_in_separate_groups() -> Result<()> {
    let mut fixture = fixture().await?;
    for _ in 0..2 {
        append(&mut fixture, Some(1), &[row(Some(1), Some("old"))]).await?;
    }
    evolve(&mut fixture).await?;
    for _ in 0..2 {
        append(&mut fixture, Some(1), &[row(Some(1), Some("new"))]).await?;
    }
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| (
                group.partition_spec_id(),
                group.partition().clone(),
                group.input_files().len()
            ))
            .collect::<Vec<_>>(),
        vec![(0, partition(Some(1)), 2), (1, partition(Some(1)), 2)]
    );
    let result = plan.execute().await?;
    let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    let summary = &fresh
        .metadata()
        .current_snapshot()
        .ok_or_else(missing)?
        .summary()
        .additional_properties;
    let partition_metrics = ["partitions.old_id=1", "partitions.new_id=1"].map(|key| {
        summary
            .get(key)
            .map(|value| value.split(',').map(str::to_string).collect::<HashSet<_>>())
    });
    for metrics in partition_metrics {
        let metrics = metrics.ok_or_else(missing)?;
        assert!(metrics.contains("added-data-files=1"));
        assert!(metrics.contains("deleted-data-files=2"));
    }
    assert_eq!(
        (
            file_states(&fresh).await?,
            read_rows(&fresh, None).await?,
            read_rows(&fresh, fixture.table.metadata().current_snapshot_id()).await?
        ),
        (
            HashSet::from([state(0, Some(1), 2, (1, 5)), state(1, Some(1), 2, (3, 5))]),
            vec![
                row(Some(1), Some("new")),
                row(Some(1), Some("new")),
                row(Some(1), Some("old")),
                row(Some(1), Some("old"))
            ],
            read_rows(&fixture.table, None).await?
        )
    );
    Ok(())
}

#[tokio::test]
async fn compacted_files_remain_readable_through_partition_pruning() -> Result<()> {
    use crate::expr::Reference;
    use crate::spec::Datum;

    let fixture = seed_groups().await?;
    let tx = Transaction::new(&fixture.table);
    let fresh = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    for value in [Some(1), Some(2), None] {
        let predicate = value.map_or_else(
            || Reference::new("id").is_null(),
            |value| Reference::new("id").equal_to(Datum::int(value)),
        );
        let scan = fresh.scan().with_filter(predicate).build()?;
        assert_eq!(read_scan_rows(scan).await?, vec![
            row(
                value,
                Some("duplicate")
            );
            2
        ]);
    }
    Ok(())
}

#[tokio::test]
async fn transformed_partitions_preserve_distinct_source_values() -> Result<()> {
    use crate::expr::Reference;
    use crate::spec::Datum;

    let mut fixture = fixture_with_transform(Transform::Truncate(10)).await?;
    for (value, id) in [(10, 11), (10, 19), (20, 21), (20, 29)] {
        append(&mut fixture, Some(value), &[row(Some(id), None)]).await?;
    }
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| group.partition().clone())
            .collect::<Vec<_>>(),
        vec![partition(Some(10)), partition(Some(20))]
    );
    let result = plan.execute().await?;
    let prepared = result.prepare(&fixture.table).await?.ok_or_else(missing)?;
    let fresh = prepared.publish(&fixture.catalog).await?;
    let filtered = fresh
        .scan()
        .with_filter(Reference::new("id").equal_to(Datum::int(19)))
        .build()?;
    assert_eq!(
        (
            read_rows(&fresh, None).await?,
            read_scan_rows(filtered).await?,
            file_states(&fresh).await?
        ),
        (
            vec![
                row(Some(11), None),
                row(Some(19), None),
                row(Some(21), None),
                row(Some(29), None)
            ],
            vec![row(Some(19), None)],
            HashSet::from([state(0, Some(10), 2, (1, 5)), state(0, Some(20), 2, (3, 5))])
        )
    );
    Ok(())
}

#[tokio::test]
async fn compound_partition_values_are_grouped_by_the_complete_tuple() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let schema = fixture.table.metadata().current_schema().clone();
    let spec = PartitionSpec::builder(schema.clone())
        .add_partition_field("id", "partition_id", Transform::Identity)?
        .add_partition_field("payload", "partition_text", Transform::Identity)?
        .build()?;
    fixture.table = fixture
        .catalog
        .create_table(
            fixture.table.identifier().namespace(),
            TableCreation::builder()
                .name("compound".to_string())
                .schema(schema.as_ref().clone())
                .partition_spec(spec.into_unbound())
                .build(),
        )
        .await?;
    let a = Struct::from_iter([Some(Literal::int(1)), Some(Literal::string("a"))]);
    let b = Struct::from_iter([Some(Literal::int(1)), None]);
    for _ in 0..2 {
        append_key(&mut fixture, a.clone(), &[row(Some(1), Some("a"))]).await?;
    }
    for _ in 0..2 {
        append_key(&mut fixture, b.clone(), &[row(Some(1), None)]).await?;
    }
    let plan = Transaction::new(&fixture.table)
        .rewrite_data_files()
        .plan()
        .await?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| group.partition().clone())
            .collect::<Vec<_>>(),
        vec![a.clone(), b.clone()]
    );
    let result = plan.execute().await?;
    let fresh = result
        .prepare(&fixture.table)
        .await?
        .ok_or_else(missing)?
        .publish(&fixture.catalog)
        .await?;
    let expected_files = HashSet::from([
        FileState {
            spec: 0,
            partition: a,
            records: 2,
            data_sequence: Some(1),
            file_sequence: Some(5),
        },
        FileState {
            spec: 0,
            partition: b,
            records: 2,
            data_sequence: Some(3),
            file_sequence: Some(5),
        },
    ]);
    assert_eq!(
        (file_states(&fresh).await?, read_rows(&fresh, None).await?),
        (expected_files, vec![
            row(Some(1), None),
            row(Some(1), None),
            row(Some(1), Some("a")),
            row(Some(1), Some("a"))
        ])
    );
    Ok(())
}

#[tokio::test]
async fn total_plan_limits_span_partition_groups_and_singletons() -> Result<()> {
    let fixture = seed_groups().await?;
    let files = live_entries(&fixture.table).await?;
    let bytes = files
        .iter()
        .map(|entry| entry.data_file().file_size_in_bytes())
        .sum::<u64>();
    let action = Transaction::new(&fixture.table).rewrite_data_files();
    let plan = action.clone().with_plan_limits(7, bytes)?.plan().await?;
    assert_eq!(
        plan.groups()
            .iter()
            .map(|group| group.input_files().len())
            .collect::<Vec<_>>(),
        vec![2, 2, 2, 1]
    );
    assert!(plan.artifacts().paths().is_empty());
    let data = super::controlled::fixture_files(&fixture.table, "data")?;
    let metadata = super::controlled::fixture_files(&fixture.table, "metadata")?;
    for (count, size) in [(6, bytes), (7, bytes - 1)] {
        let error = action
            .clone()
            .with_plan_limits(count, size)?
            .plan()
            .await
            .err()
            .ok_or_else(missing)?;
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }
    assert_eq!(
        (
            super::controlled::fixture_files(&fixture.table, "data")?,
            super::controlled::fixture_files(&fixture.table, "metadata")?
        ),
        (data, metadata)
    );
    Ok(())
}

#[tokio::test]
async fn partitioned_equality_deletes_materialize_without_hiding_newer_values() -> Result<()> {
    use super::delete_fixture::{equality, publish};
    let mut fixture = fixture().await?;
    append(&mut fixture, Some(1), &[row(Some(1), Some("old"))]).await?;
    let key = PartitionKey::new(
        fixture
            .table
            .metadata()
            .default_partition_spec()
            .as_ref()
            .clone(),
        fixture.table.metadata().current_schema().clone(),
        partition(Some(1)),
    );
    let deletes = equality(&fixture.table, &[Some(1)], Some(key)).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    append(&mut fixture, Some(1), &[row(Some(1), Some("new"))]).await?;
    append(&mut fixture, Some(2), &[row(Some(2), Some("untouched"))]).await?;
    let selected = live_entries(&fixture.table)
        .await?
        .into_iter()
        .filter(|entry| {
            entry.content_type() == crate::spec::DataContentType::Data
                && entry.data_file().partition() == &partition(Some(1))
        })
        .map(|entry| entry.data_file().clone())
        .collect::<Vec<_>>();
    let tx = Transaction::new(&fixture.table);
    let fresh = tx
        .rewrite_data_files()
        .select_data_files(selected)
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, vec![
        row(Some(1), Some("new")),
        row(Some(2), Some("untouched"))
    ]);
    Ok(())
}

#[tokio::test]
async fn global_equality_deletes_span_old_and_new_partition_specs() -> Result<()> {
    use super::delete_fixture::{equality, publish};
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[row(Some(1), Some("old-unpartitioned"))])
        .await?;
    evolve(&mut fixture).await?;
    append(&mut fixture, Some(1), &[row(
        Some(1),
        Some("old-partitioned"),
    )])
    .await?;
    let deletes = equality(&fixture.table, &[Some(1)], None).await?;
    publish(&mut fixture, &deletes, Operation::Delete).await?;
    append(&mut fixture, Some(1), &[row(Some(1), Some("new"))]).await?;
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

#[tokio::test]
async fn row_delta_groups_new_data_and_global_deletes_by_their_actual_specs() -> Result<()> {
    use super::delete_fixture::equality;
    let mut fixture = Fixture::new().await?;
    fixture.append(&[row(Some(1), Some("old"))]).await?;
    evolve(&mut fixture).await?;
    let key = PartitionKey::new(
        fixture
            .table
            .metadata()
            .default_partition_spec()
            .as_ref()
            .clone(),
        fixture.table.metadata().current_schema().clone(),
        partition(Some(1)),
    );
    let data =
        write_partition_rows(&fixture.table, &[row(Some(1), Some("new"))], Some(key)).await?;
    let deletes = equality(&fixture.table, &[Some(1)], None).await?;
    let prepared = Transaction::new(&fixture.table)
        .row_delta()
        .add_data_files(data)
        .add_delete_files(deletes)
        .prepare(&fixture.table)
        .await?;
    let id = prepared.snapshot().snapshot_id();
    let fresh = prepared.publish(&fixture.catalog).await?;
    let mut groups = crate::transaction::snapshot::current_manifests(&fresh)
        .await?
        .into_iter()
        .filter(|manifest| manifest.added_snapshot_id == id)
        .map(|manifest| {
            (
                manifest.partition_spec_id,
                manifest.content == crate::spec::ManifestContentType::Data,
            )
        })
        .collect::<Vec<_>>();
    groups.sort_unstable();
    assert_eq!(
        (read_rows(&fresh, None).await?, groups),
        (vec![row(Some(1), Some("new"))], vec![(0, false), (1, true)])
    );
    Ok(())
}

#[tokio::test]
async fn row_delta_rejects_nonprimitive_partition_literals_without_panicking() -> Result<()> {
    let fixture = fixture().await?;
    let key = PartitionKey::new(
        fixture
            .table
            .metadata()
            .default_partition_spec()
            .as_ref()
            .clone(),
        fixture.table.metadata().current_schema().clone(),
        partition(Some(1)),
    );
    let data = write_partition_rows(&fixture.table, &[row(Some(1), None)], Some(key)).await?;
    for literal in [Literal::List(vec![]), Literal::Struct(Struct::empty())] {
        let mut files = data.clone();
        files.first_mut().ok_or_else(missing)?.partition = Struct::from_iter([Some(literal)]);
        let action = Transaction::new(&fixture.table)
            .row_delta()
            .add_data_files(files);
        let error = action
            .prepare(&fixture.table)
            .await
            .err()
            .ok_or_else(missing)?;
        assert_eq!(
            (error.kind(), action.artifacts().paths()),
            (ErrorKind::DataInvalid, vec![])
        );
    }
    Ok(())
}
