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
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{DataFile, DataFileFormat, NestedField, PrimitiveType, Schema};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, RewriteDataFilesResult, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, NamespaceIdent, Result, TableCreation};

pub struct Demo {
    pub catalog: MemoryCatalog,
    pub warehouse: PathBuf,
}

impl Demo {
    pub async fn new() -> Result<Self> {
        // Retain this demo-owned warehouse even if a submission outcome is unknown.
        let warehouse = tempfile::tempdir()?.keep();
        println!("Disposable local warehouse: {}", warehouse.display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "demo",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await?;
        catalog
            .create_namespace(
                &NamespaceIdent::new("compaction".to_string()),
                HashMap::new(),
            )
            .await?;
        Ok(Self { catalog, warehouse })
    }

    pub async fn seed(&self, name: &str) -> Result<(Table, Vec<DataFile>)> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "value", PrimitiveType::Int).into(),
            ])
            .build()?;
        let table = self
            .catalog
            .create_table(
                &NamespaceIdent::new("compaction".to_string()),
                TableCreation::builder()
                    .name(name.to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;
        let (table, mut selected) = self.append(&table, &[None, Some(1)]).await?;
        let (table, second) = self.append(&table, &[Some(1), Some(2)]).await?;
        selected.extend(second);
        let (table, _) = self.append(&table, &[Some(99)]).await?;
        Ok((table, selected))
    }

    pub async fn append(
        &self,
        table: &Table,
        values: &[Option<i32>],
    ) -> Result<(Table, Vec<DataFile>)> {
        let files = Self::write(table, values, false).await?;
        let tx = Transaction::new(table);
        let appended = tx
            .fast_append()
            .add_data_files(files.clone())
            .apply(tx)?
            .commit(&self.catalog)
            .await?;
        Ok((appended, files))
    }
    pub async fn write(
        table: &Table,
        values: &[Option<i32>],
        equality: bool,
    ) -> Result<Vec<DataFile>> {
        let schema = table.metadata().current_schema().clone();
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::try_from(schema.as_ref())?), vec![
            Arc::new(values.iter().copied().collect::<Int32Array>()),
        ])?;
        let parquet = ParquetWriterBuilder::from_table_properties(
            &table.metadata().table_properties(),
            schema,
        )?;
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata())?,
            DefaultFileNameGenerator::new(
                format!("demo-{}", uuid::Uuid::new_v4()),
                None,
                DataFileFormat::Parquet,
            ),
        );
        if equality {
            let config = EqualityDeleteWriterConfig::new(
                vec![1],
                table.metadata().current_schema().clone(),
            )?;
            let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
                .build(None)
                .await?;
            writer.write(batch).await?;
            writer.close().await
        } else {
            let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
            writer.write(batch).await?;
            writer.close().await
        }
    }
}

pub fn missing() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Expected compaction metadata or outcome",
    )
}

pub fn data_paths(result: &RewriteDataFilesResult) -> Vec<String> {
    result
        .artifacts()
        .paths()
        .into_iter()
        .filter(|path| {
            std::path::Path::new(path)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
        })
        .collect()
}

pub async fn rows(table: &Table, snapshot: Option<i64>) -> Result<Vec<Option<i32>>> {
    let mut scan = table.scan();
    if let Some(id) = snapshot {
        scan = scan.snapshot_id(id);
    }
    let mut stream = scan.build()?.to_arrow().await?;
    let mut rows = vec![];
    while let Some(batch) = stream.try_next().await? {
        let values = batch.column(0).as_primitive::<Int32Type>();
        rows.extend(
            (0..values.len()).map(|index| (!values.is_null(index)).then(|| values.value(index))),
        );
    }
    rows.sort();
    Ok(rows)
}
