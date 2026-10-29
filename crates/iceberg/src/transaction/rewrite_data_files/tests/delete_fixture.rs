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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema as ArrowSchema;
use parquet::file::properties::WriterProperties;

use super::Fixture;
use crate::arrow::arrow_schema_to_schema;
use crate::spec::{
    DataFile, DataFileFormat, ManifestContentType, Operation, PartitionKey, SchemaRef,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, new_manifest_writer, write_snapshot_commit,
};
use crate::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use crate::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, position_delete_schema,
};
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Catalog, Error, ErrorKind, Result, TableCommit};

type Rolling = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

pub(super) fn missing() -> Error {
    Error::new(ErrorKind::Unexpected, "Expected delete compaction metadata")
}

fn rolling(table: &Table, schema: SchemaRef) -> Result<Rolling> {
    Ok(RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(
            format!("delete-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        ),
    ))
}

pub(super) async fn equality(
    table: &Table,
    keys: &[Option<i32>],
    partition: Option<PartitionKey>,
) -> Result<Vec<DataFile>> {
    let schema = table.metadata().current_schema().clone();
    let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())?;
    let delete_schema = Arc::new(arrow_schema_to_schema(config.projected_arrow_schema_ref())?);
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::try_from(schema.as_ref())?), vec![
        Arc::new(keys.iter().copied().collect::<Int32Array>()),
        Arc::new(keys.iter().map(|_| None::<&str>).collect::<StringArray>()),
    ])?;
    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling(table, delete_schema)?, config)
        .build(partition)
        .await?;
    writer.write(batch).await?;
    writer.close().await
}

pub(super) async fn positions(table: &Table, positions: &[(String, i64)]) -> Result<Vec<DataFile>> {
    let mut positions = positions.to_vec();
    positions.sort();
    let schema = position_delete_schema();
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::try_from(schema.as_ref())?), vec![
        Arc::new(
            positions
                .iter()
                .map(|(path, _)| Some(path.as_str()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            positions
                .iter()
                .map(|(_, position)| *position)
                .collect::<Int64Array>(),
        ),
    ])?;
    let mut writer = PositionDeleteFileWriterBuilder::new(rolling(table, schema)?)
        .build(None)
        .await?;
    writer.write(batch).await?;
    writer.close().await
}

pub(super) async fn publish(
    fixture: &mut Fixture,
    files: &[DataFile],
    operation: Operation,
) -> Result<()> {
    let table = &fixture.table;
    let id = generate_snapshot_id(table);
    let uuid = uuid::Uuid::now_v7();
    let mut manifests = current_manifests(table).await?;
    let mut groups: BTreeMap<_, Vec<&DataFile>> = BTreeMap::new();
    for file in files {
        groups
            .entry((
                file.partition_spec_id,
                file.content_type() == crate::spec::DataContentType::Data,
            ))
            .or_default()
            .push(file);
    }
    for (index, ((spec_id, data), files)) in groups.into_iter().enumerate() {
        let spec = table
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(missing)?
            .clone();
        let mut writer = new_manifest_writer(
            table,
            manifest_path(table, uuid, u64::try_from(index)?)?,
            id,
            if data {
                ManifestContentType::Data
            } else {
                ManifestContentType::Deletes
            },
            table.metadata().current_schema().clone(),
            spec,
        )?;
        for file in files {
            writer.add_file(file.clone(), -1)?;
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    let mut action = write_snapshot_commit(table, SnapshotCommitInput {
        snapshot_id: id,
        manifest_list_path: manifest_list_path(table, id, 0, uuid)?,
        operation,
        properties: HashMap::new(),
        added_files: files,
        removed_files: &[],
        manifests,
        truncate_full_table: false,
    })
    .await?;
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(action.take_updates())
        .requirements(action.take_requirements())
        .build();
    fixture.table = fixture.catalog.update_table(commit).await?;
    Ok(())
}

pub(super) async fn replace_inventory(
    table: &Table,
    entries: Vec<crate::spec::ManifestEntry>,
) -> Result<Table> {
    let id = generate_snapshot_id(table);
    let uuid = uuid::Uuid::now_v7();
    let mut manifests = current_manifests(table)
        .await?
        .into_iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
        .collect::<Vec<_>>();
    if !entries.is_empty() {
        let mut writer = new_manifest_writer(
            table,
            manifest_path(table, uuid, 0)?,
            id,
            ManifestContentType::Deletes,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().clone(),
        )?;
        for entry in entries {
            writer.add_existing_entry(entry)?;
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    let mut action = write_snapshot_commit(table, SnapshotCommitInput {
        snapshot_id: id,
        manifest_list_path: manifest_list_path(table, id, 0, uuid)?,
        operation: Operation::Append,
        properties: HashMap::new(),
        added_files: &[],
        removed_files: &[],
        manifests,
        truncate_full_table: false,
    })
    .await?;
    crate::transaction::Transaction::update_table_metadata(table.clone(), &action.take_updates())
}
