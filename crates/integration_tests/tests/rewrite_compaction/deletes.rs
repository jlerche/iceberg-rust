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

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema as ArrowSchema;
use iceberg::Result;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::{DataFile, DataFileFormat, Literal, PartitionKey, SchemaRef, Struct};
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, position_delete_schema,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};

use super::fixture::Fixture;

type Rolling = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

fn key(fixture: &Fixture, id: Option<i32>) -> PartitionKey {
    PartitionKey::new(
        fixture
            .table
            .metadata()
            .default_partition_spec()
            .as_ref()
            .clone(),
        fixture.table.metadata().current_schema().clone(),
        Struct::from_iter([id.map(Literal::int)]),
    )
}

fn rolling(fixture: &Fixture, schema: SchemaRef) -> Result<Rolling> {
    Ok(RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::from_table_properties(
            &fixture.table.metadata().table_properties(),
            schema,
        )?,
        fixture.table.file_io().clone(),
        DefaultLocationGenerator::new(fixture.table.metadata())?,
        DefaultFileNameGenerator::new(
            format!("delete-{}", uuid::Uuid::new_v4()),
            None,
            DataFileFormat::Parquet,
        ),
    ))
}

pub async fn equality(fixture: &Fixture, id: Option<i32>) -> Result<Vec<DataFile>> {
    let schema = fixture.table.metadata().current_schema().clone();
    let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())?;
    let delete_schema = Arc::new(arrow_schema_to_schema(config.projected_arrow_schema_ref())?);
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::try_from(schema.as_ref())?), vec![
        Arc::new(Int32Array::from(vec![id])),
        Arc::new(StringArray::from(vec![None::<&str>])),
    ])?;
    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling(fixture, delete_schema)?, config)
        .build(Some(key(fixture, id)))
        .await?;
    writer.write(batch).await?;
    writer.close().await
}

pub async fn positions(
    fixture: &Fixture,
    id: Option<i32>,
    mut pairs: Vec<(String, i64)>,
) -> Result<Vec<DataFile>> {
    pairs.sort();
    let schema = position_delete_schema();
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::try_from(schema.as_ref())?), vec![
        Arc::new(
            pairs
                .iter()
                .map(|(path, _)| Some(path.as_str()))
                .collect::<StringArray>(),
        ),
        Arc::new(pairs.iter().map(|(_, pos)| *pos).collect::<Int64Array>()),
    ])?;
    let mut writer = PositionDeleteFileWriterBuilder::new(rolling(fixture, schema)?)
        .build(Some(key(fixture, id)))
        .await?;
    writer.write(batch).await?;
    writer.close().await
}
