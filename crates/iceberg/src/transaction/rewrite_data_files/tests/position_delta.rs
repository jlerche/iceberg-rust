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

use super::delete_fixture::{equality, missing, positions, positions_ordered};
use super::{Fixture, live_entries, read_rows, row, write_rows};
use crate::spec::DataFile;
use crate::transaction::{ApplyTransactionAction, RowDeltaAction, Transaction};
use crate::{Catalog, ErrorKind, Result};

async fn fixture() -> Result<Fixture> {
    let mut fixture = Fixture::new().await?;
    fixture
        .append(&[
            row(Some(10), None),
            row(Some(20), None),
            row(Some(30), None),
            row(Some(40), None),
        ])
        .await?;
    Ok(fixture)
}

async fn target(fixture: &Fixture) -> Result<DataFile> {
    live_entries(&fixture.table)
        .await?
        .iter()
        .find(|entry| entry.content_type() == crate::spec::DataContentType::Data)
        .map(|entry| entry.data_file().clone())
        .ok_or_else(missing)
}

async fn rejects(action: &RowDeltaAction, fixture: &Fixture, kind: ErrorKind) -> Result<()> {
    let before = action.artifacts().paths();
    let error = action
        .prepare(&fixture.table)
        .await
        .err()
        .ok_or_else(missing)?;
    assert_eq!((error.kind(), action.artifacts().paths()), (kind, before));
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(fresh.metadata(), fixture.table.metadata());
    Ok(())
}

#[tokio::test]
async fn position_delta_uses_raw_positions_despite_existing_deletes() -> Result<()> {
    let mut fixture = fixture().await?;
    let file = target(&fixture).await?;
    let eq = equality(&fixture.table, &[Some(10)], None).await?;
    fixture.table = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files(eq)
        .prepare(&fixture.table)
        .await?
        .publish(&fixture.catalog)
        .await?;
    let base = fixture.table.clone();
    let deletes = positions(&base, &[
        (file.file_path().to_string(), 1),
        (file.file_path().to_string(), 3),
    ])
    .await?;
    let data = write_rows(&base, &[row(Some(50), None)]).await?;
    let prepared = Transaction::new(&base)
        .row_delta()
        .add_data_files(data)
        .add_delete_files(deletes)
        .prepare(&base)
        .await?;
    let fresh = fixture
        .catalog
        .update_table(prepared.try_into_table_commit()?)
        .await?;
    assert_eq!(
        (
            read_rows(&fresh, None).await?,
            read_rows(&fresh, base.metadata().current_snapshot_id()).await?
        ),
        (vec![row(Some(30), None), row(Some(50), None)], vec![
            row(Some(20), None),
            row(Some(30), None),
            row(Some(40), None)
        ])
    );
    let tx = Transaction::new(&fresh);
    let compacted = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&compacted, None).await?, vec![
        row(Some(30), None),
        row(Some(50), None)
    ]);
    Ok(())
}

#[tokio::test]
async fn shared_position_delta_deletes_live_and_same_commit_new_rows() -> Result<()> {
    let mut fixture = fixture().await?;
    fixture.append(&[row(Some(60), None)]).await?;
    let old = target(&fixture).await?;
    let data = write_rows(&fixture.table, &[row(Some(70), None), row(Some(80), None)]).await?;
    let new = data.first().ok_or_else(missing)?;
    let deletes = positions(&fixture.table, &[
        (old.file_path().to_string(), 0),
        (new.file_path().to_string(), 1),
    ])
    .await?;
    let tx = Transaction::new(&fixture.table);
    let fresh = tx
        .row_delta()
        .add_data_files(data)
        .add_delete_files(deletes)
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, vec![
        row(Some(20), None),
        row(Some(30), None),
        row(Some(40), None),
        row(Some(60), None),
        row(Some(70), None)
    ]);
    Ok(())
}

#[tokio::test]
async fn position_delta_rejects_unknown_paths_bounds_order_and_duplicate_pairs() -> Result<()> {
    let fixture = fixture().await?;
    let file = target(&fixture).await?;
    let path = file.file_path().to_string();
    for pairs in [
        vec![("missing.parquet".to_string(), 0)],
        vec![(path.clone(), -1)],
        vec![(path.clone(), 4)],
        vec![(path.clone(), 2), (path.clone(), 1)],
        vec![(path.clone(), 1), (path, 1)],
    ] {
        let deletes = positions_ordered(&fixture.table, &pairs, None).await?;
        rejects(
            &Transaction::new(&fixture.table)
                .row_delta()
                .add_delete_files(deletes),
            &fixture,
            ErrorKind::DataInvalid,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn position_delta_rejects_ordering_resets_across_batches() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let rows = (0..1030).map(|id| row(Some(id), None)).collect::<Vec<_>>();
    fixture.append(&rows).await?;
    let file = target(&fixture).await?;
    let mut pairs = (0..1024)
        .map(|pos| (file.file_path().to_string(), pos))
        .collect::<Vec<_>>();
    pairs.push((file.file_path().to_string(), 0));
    let deletes = positions_ordered(&fixture.table, &pairs, None).await?;
    rejects(
        &Transaction::new(&fixture.table)
            .row_delta()
            .add_delete_files(deletes),
        &fixture,
        ErrorKind::DataInvalid,
    )
    .await
}

#[tokio::test]
async fn position_delta_checks_referenced_metadata_and_real_delete_file_counts_and_size()
-> Result<()> {
    let fixture = fixture().await?;
    let file = target(&fixture).await?;
    let deletes = positions(&fixture.table, &[(file.file_path().to_string(), 0)]).await?;
    let delete = deletes.first().ok_or_else(missing)?.clone();
    let mut reference = delete.clone();
    reference.referenced_data_file = Some("wrong.parquet".to_string());
    let mut count = delete.clone();
    count.record_count += 1;
    let mut size = delete.clone();
    size.file_size_in_bytes += 1;
    let mut ids = delete.clone();
    ids.equality_ids = Some(vec![1]);
    let mut sort = delete.clone();
    sort.sort_order_id = Some(0);
    for file in [reference, count, size, ids, sort] {
        rejects(
            &Transaction::new(&fixture.table)
                .row_delta()
                .add_delete_files([file]),
            &fixture,
            ErrorKind::DataInvalid,
        )
        .await?;
    }
    let mut scoped = delete;
    scoped.referenced_data_file = Some(file.file_path().to_string());
    let fresh = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files([scoped])
        .prepare(&fixture.table)
        .await?
        .publish(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, vec![
        row(Some(20), None),
        row(Some(30), None),
        row(Some(40), None)
    ]);
    Ok(())
}

#[tokio::test]
async fn position_delta_checks_new_target_counts_against_physical_parquet() -> Result<()> {
    let fixture = Fixture::new().await?;
    let mut data = write_rows(&fixture.table, &[row(Some(1), None)]).await?;
    let file = data.first_mut().ok_or_else(missing)?;
    file.record_count = 10;
    let deletes = positions(&fixture.table, &[(file.file_path().to_string(), 5)]).await?;
    rejects(
        &Transaction::new(&fixture.table)
            .row_delta()
            .add_data_files(data)
            .add_delete_files(deletes),
        &fixture,
        ErrorKind::DataInvalid,
    )
    .await
}

#[tokio::test]
async fn position_delta_requires_replanning_after_compaction() -> Result<()> {
    let mut fixture = fixture().await?;
    fixture.append(&[row(Some(50), None)]).await?;
    let file = target(&fixture).await?;
    let deletes = positions(&fixture.table, &[(file.file_path().to_string(), 0)]).await?;
    let action = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files(deletes.clone());
    let prepared = action.prepare(&fixture.table).await?;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .rewrite_data_files()
        .apply(tx)?
        .commit(&fixture.catalog)
        .await?;
    rejects(&action, &fixture, ErrorKind::PreconditionFailed).await?;
    let fresh_action = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files(deletes);
    rejects(&fresh_action, &fixture, ErrorKind::DataInvalid).await?;
    assert!(
        fixture
            .catalog
            .update_table(prepared.try_into_table_commit()?)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn position_delta_rejects_wrong_physical_schema_ids_and_nullability() -> Result<()> {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::metadata_columns::{
        RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
    };
    let fixture = fixture().await?;
    let file = target(&fixture).await?;
    for (id, nullable) in [(10, false), (RESERVED_FIELD_ID_DELETE_FILE_PATH, true)] {
        let schema = Schema::new(vec![
            Field::new("file_path", DataType::Utf8, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_DELETE_FILE_POS.to_string(),
            )])),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(StringArray::from(vec![file.file_path()])),
            Arc::new(Int64Array::from(vec![0])),
        ])?;
        let deletes = super::delete_fixture::position_batch(&fixture.table, batch).await?;
        rejects(
            &Transaction::new(&fixture.table)
                .row_delta()
                .add_delete_files(deletes),
            &fixture,
            ErrorKind::DataInvalid,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn position_delta_unreadable_payload_does_not_allocate_metadata_or_delete_inputs()
-> Result<()> {
    let fixture = fixture().await?;
    let file = target(&fixture).await?;
    let deletes = positions(&fixture.table, &[(file.file_path().to_string(), 0)]).await?;
    let path = deletes.first().ok_or_else(missing)?.file_path().to_string();
    fixture.table.file_io().delete(path).await?;
    let action = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files(deletes);
    assert!(action.prepare(&fixture.table).await.is_err());
    assert!(action.artifacts().paths().is_empty());
    assert!(
        fixture
            .table
            .file_io()
            .new_input(file.file_path())?
            .exists()
            .await?
    );
    let fresh = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await?;
    assert_eq!(fresh.metadata(), fixture.table.metadata());
    Ok(())
}

#[tokio::test]
async fn position_delta_allows_overlapping_positions_in_distinct_delete_files() -> Result<()> {
    let fixture = fixture().await?;
    let file = target(&fixture).await?;
    let pairs = [(file.file_path().to_string(), 0)];
    let mut deletes = positions(&fixture.table, &pairs).await?;
    deletes.extend(positions(&fixture.table, &pairs).await?);
    let fresh = Transaction::new(&fixture.table)
        .row_delta()
        .add_delete_files(deletes)
        .prepare(&fixture.table)
        .await?
        .publish(&fixture.catalog)
        .await?;
    assert_eq!(read_rows(&fresh, None).await?, vec![
        row(Some(20), None),
        row(Some(30), None),
        row(Some(40), None)
    ]);
    Ok(())
}

#[tokio::test]
async fn position_delta_rejects_binary_path_hidden_by_arrow_string_metadata() -> Result<()> {
    use std::sync::Arc;

    use arrow_array::{BinaryArray, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::{ARROW_SCHEMA_META_KEY, ArrowWriter, encode_arrow_schema};
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    let fixture = fixture().await?;
    let target = target(&fixture).await?;
    let hint = crate::writer::base_writer::position_delete_writer::position_delete_arrow_schema();
    let physical = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Binary, false)
            .with_metadata(hint.field(0).metadata().clone()),
        hint.field(1).clone(),
    ]));
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue {
            key: ARROW_SCHEMA_META_KEY.to_string(),
            value: Some(encode_arrow_schema(hint.as_ref())),
        }]))
        .build();
    let options = ArrowWriterOptions::new()
        .with_properties(properties)
        .with_skip_arrow_metadata(true);
    let batch = RecordBatch::try_new(physical.clone(), vec![
        Arc::new(BinaryArray::from_vec(vec![target.file_path().as_bytes()])),
        Arc::new(Int64Array::from(vec![0])),
    ])?;
    let mut bytes = vec![];
    let mut writer = ArrowWriter::try_new_with_options(&mut bytes, physical, options)?;
    writer.write(&batch)?;
    writer.close()?;
    let mut file = positions(&fixture.table, &[(target.file_path().to_string(), 0)])
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    file.file_path.push_str("-binary.parquet");
    file.file_size_in_bytes = u64::try_from(bytes.len())?;
    fixture
        .table
        .file_io()
        .new_output(file.file_path())?
        .write(bytes.into())
        .await?;
    rejects(
        &Transaction::new(&fixture.table)
            .row_delta()
            .add_delete_files([file]),
        &fixture,
        ErrorKind::DataInvalid,
    )
    .await
}
