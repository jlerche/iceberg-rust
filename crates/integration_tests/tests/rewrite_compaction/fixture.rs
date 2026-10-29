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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema as ArrowSchema;
use futures::TryStreamExt;
use iceberg::io::CancellationToken;
use iceberg::spec::{
    DataFile, DataFileFormat, Datum, Literal, ManifestEntryRef, NestedField, PartitionKey,
    PartitionSpec, PrimitiveType, Schema, Struct, Transform,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, NamespaceIdent, Result, TableCreation};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

use super::faults::{FaultControl, FaultStorageFactory};

pub type Row = (Option<i32>, Option<String>);

pub struct Fixture {
    pub catalog: Arc<RestCatalog>,
    pub table: Table,
    pub faults: FaultControl,
    pub cancellation: CancellationToken,
}

pub fn row(id: Option<i32>, text: Option<&str>) -> Row {
    (id, text.map(str::to_string))
}
pub fn missing() -> Error {
    Error::new(ErrorKind::Unexpected, "Missing compaction test metadata")
}

impl Fixture {
    pub async fn new() -> Result<Self> {
        let faults = FaultControl::default();
        let cancellation = faults.cancellation.clone();
        let factory = FaultStorageFactory {
            inner: OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            },
            faults: faults.clone(),
        };
        let catalog = Arc::new(
            RestCatalogBuilder::default()
                .with_storage_factory(Arc::new(factory))
                .load("rest", get_test_fixture().catalog_config.clone())
                .await?,
        );
        let namespace = NamespaceIdent::new(format!("rewrite_{}", uuid::Uuid::new_v4().simple()));
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", PrimitiveType::Int).into(),
                NestedField::optional(2, "payload", PrimitiveType::String).into(),
            ])
            .build()?;
        let spec = PartitionSpec::builder(Arc::new(schema.clone()))
            .add_partition_field("id", "old_id", Transform::Identity)?
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".to_string())
                    .schema(schema)
                    .partition_spec(spec.into_unbound())
                    .build(),
            )
            .await?;
        Ok(Self {
            catalog,
            table,
            faults,
            cancellation,
        })
    }

    pub async fn write(&self, rows: &[Row]) -> Result<Vec<DataFile>> {
        let value = rows.first().ok_or_else(missing)?.0;
        assert!(rows.iter().all(|row| row.0 == value));
        let schema = self.table.metadata().current_schema().clone();
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::try_from(schema.as_ref())?), vec![
            Arc::new(rows.iter().map(|row| row.0).collect::<Int32Array>()),
            Arc::new(
                rows.iter()
                    .map(|row| row.1.as_deref())
                    .collect::<StringArray>(),
            ),
        ])?;
        let parquet = ParquetWriterBuilder::from_table_properties(
            &self.table.metadata().table_properties(),
            schema.clone(),
        )?;
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            self.table.file_io().clone(),
            DefaultLocationGenerator::new(self.table.metadata())?,
            DefaultFileNameGenerator::new(
                format!("test-{}", uuid::Uuid::new_v4()),
                None,
                DataFileFormat::Parquet,
            ),
        );
        let key = PartitionKey::new(
            self.table
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone(),
            schema,
            Struct::from_iter([value.map(Literal::int)]),
        );
        let mut writer = DataFileWriterBuilder::new(rolling).build(Some(key)).await?;
        writer.write(batch).await?;
        writer.close().await
    }

    pub async fn append(&mut self, rows: &[Row]) -> Result<()> {
        let files = self.write(rows).await?;
        let tx = Transaction::new(&self.table);
        self.table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)?
            .commit(self.catalog.as_ref())
            .await?;
        Ok(())
    }

    pub async fn seed(&mut self) -> Result<()> {
        for value in [None, Some(1)] {
            for _ in 0..2 {
                self.append(&[row(value, Some("duplicate"))]).await?;
            }
        }
        Ok(())
    }

    pub async fn fresh(&self) -> Result<Table> {
        self.catalog.load_table(self.table.identifier()).await
    }

    pub async fn cleanup(self) -> Result<()> {
        let namespace = self.table.identifier().namespace().clone();
        let marker = format!("/{}/", namespace.to_url_string());
        assert!(self.table.metadata().location().contains(&marker));
        self.catalog.drop_table(self.table.identifier()).await?;
        // The UUID namespace and table are created by this fixture alone.
        self.table
            .file_io()
            .delete_prefix(format!(
                "{}/",
                self.table.metadata().location().trim_end_matches('/')
            ))
            .await?;
        self.catalog.drop_namespace(&namespace).await
    }
}

pub async fn read(table: &Table, snapshot: Option<i64>) -> Result<Vec<Row>> {
    let mut builder = table.scan();
    if let Some(snapshot) = snapshot {
        builder = builder.snapshot_id(snapshot);
    }
    decode(builder.build()?.to_arrow().await?).await
}

pub async fn filtered(table: &Table, id: i32) -> Result<Vec<Row>> {
    let scan = table
        .scan()
        .with_filter(iceberg::expr::Reference::new("id").equal_to(Datum::int(id)))
        .build()?;
    decode(scan.to_arrow().await?).await
}

async fn decode(mut batches: iceberg::scan::ArrowRecordBatchStream) -> Result<Vec<Row>> {
    let mut rows = vec![];
    while let Some(batch) = batches.try_next().await? {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let texts = batch.column(1).as_string::<i32>();
        rows.extend((0..batch.num_rows()).map(|i| {
            (
                (!ids.is_null(i)).then(|| ids.value(i)),
                (!texts.is_null(i)).then(|| texts.value(i).to_string()),
            )
        }));
    }
    rows.sort();
    Ok(rows)
}

pub async fn live(table: &Table) -> Result<Vec<(i32, ManifestEntryRef)>> {
    let snapshot = table.metadata().current_snapshot().ok_or_else(missing)?;
    let mut entries = vec![];
    for descriptor in table.manifest_list_reader(snapshot).load().await?.entries() {
        let manifest = table.manifest_reader().read(descriptor).await?;
        entries.extend(
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
                .map(|entry| (descriptor.partition_spec_id, entry.clone())),
        );
    }
    Ok(entries)
}

pub async fn retained(table: &Table, paths: Vec<String>) -> Result<()> {
    for path in paths {
        assert!(table.file_io().exists(path).await?);
    }
    Ok(())
}

pub fn run<F>(future: F) -> Result<()>
where F: Future<Output = Result<()>> + Send + 'static {
    futures::executor::block_on(iceberg_test_utils::test_runtime().io().spawn(future))?
}
