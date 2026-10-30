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

use arrow_array::{Array, Int64Array, StringArray};
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use super::validation::invalid;
use crate::arrow::{ArrowFileReader, ArrowReader, ParquetReadOptions, ScanMetrics};
use crate::spec::{DataContentType, DataFile, DataFileFormat, ManifestContentType};
use crate::table::Table;
use crate::transaction::snapshot::current_manifests;
use crate::{Error, ErrorKind, Result};

pub(super) async fn validate(table: &Table, data: &[DataFile], deletes: &[DataFile]) -> Result<()> {
    let positional = deletes
        .iter()
        .filter(|file| file.content_type() == DataContentType::PositionDeletes)
        .collect::<Vec<_>>();
    if positional.is_empty() {
        return Ok(());
    }
    let mut targets = HashMap::new();
    for manifest in current_manifests(table)
        .await?
        .into_iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
    {
        table.file_io().check_cancellation()?;
        let manifest = table.object_cache().get_manifest(&manifest).await?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            if targets
                .insert(entry.file_path().to_string(), entry.data_file().clone())
                .is_some()
            {
                return Err(invalid());
            }
        }
    }
    for file in data {
        if targets
            .insert(file.file_path().to_string(), file.clone())
            .is_some()
        {
            return Err(invalid());
        }
    }
    let mut verifier = Verifier {
        table,
        targets,
        counts: HashMap::new(),
        metrics: ScanMetrics::new(),
    };
    for file in positional {
        verifier
            .file(file)
            .await
            .map_err(|error| error.with_context("position_delete", file.file_path()))?;
    }
    Ok(())
}

struct Verifier<'a> {
    table: &'a Table,
    targets: HashMap<String, DataFile>,
    counts: HashMap<String, u64>,
    metrics: ScanMetrics,
}

impl Verifier<'_> {
    async fn file(&mut self, file: &DataFile) -> Result<()> {
        self.table.file_io().check_cancellation()?;
        if let Some(reference) = file.referenced_data_file.as_deref() {
            self.scope(file, reference)?;
        }
        let (reader, metadata) = open(self.table, file, &self.metrics).await?;
        super::position_schema::validate(&metadata)?;
        let mut batches = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
            .with_batch_size(1024)
            .build()?;
        let mut previous: Option<(String, u64)> = None;
        let mut rows = 0_u64;
        while let Some(batch) = batches.try_next().await? {
            self.table.file_io().check_cancellation()?;
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(invalid)?;
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(invalid)?;
            for index in 0..batch.num_rows() {
                self.table.file_io().check_cancellation()?;
                if paths.is_null(index) || positions.is_null(index) {
                    return Err(invalid());
                }
                let path = paths.value(index);
                let position = u64::try_from(positions.value(index))?;
                if previous
                    .as_ref()
                    .is_some_and(|(last, pos)| (path, position) <= (last.as_str(), *pos))
                {
                    return Err(invalid());
                }
                self.target(file, path, position).await?;
                previous = Some((path.to_string(), position));
            }
            rows = rows
                .checked_add(u64::try_from(batch.num_rows())?)
                .ok_or_else(invalid)?;
        }
        self.table.file_io().check_cancellation()?;
        if rows != file.record_count() {
            return Err(invalid());
        }
        Ok(())
    }

    fn scope(&self, delete: &DataFile, path: &str) -> Result<&DataFile> {
        if delete
            .referenced_data_file
            .as_deref()
            .is_some_and(|reference| reference != path)
        {
            return Err(invalid());
        }
        let target = self.targets.get(path).ok_or_else(invalid)?;
        if target.partition_spec_id != delete.partition_spec_id {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Position publication requires the target's partition spec",
            ));
        }
        if target.partition() != delete.partition() {
            return Err(invalid());
        }
        Ok(target)
    }

    async fn target(&mut self, delete: &DataFile, path: &str, position: u64) -> Result<()> {
        let target = self.scope(delete, path)?;
        let count = if let Some(count) = self.counts.get(path) {
            *count
        } else {
            let (_, metadata) = open(self.table, target, &self.metrics).await?;
            let count = u64::try_from(metadata.metadata().file_metadata().num_rows())?;
            self.counts.insert(path.to_string(), count);
            count
        };
        if position >= count {
            return Err(invalid());
        }
        Ok(())
    }
}

async fn open(
    table: &Table,
    file: &DataFile,
    metrics: &ScanMetrics,
) -> Result<(ArrowFileReader, ArrowReaderMetadata)> {
    table.file_io().check_cancellation()?;
    if file.file_format() != DataFileFormat::Parquet {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Position publication requires Parquet targets",
        ));
    }
    let size = table
        .file_io()
        .new_input(file.file_path())?
        .metadata()
        .await?
        .size;
    if size != file.file_size_in_bytes() {
        return Err(invalid());
    }
    let opened = ArrowReader::open_parquet_file(
        file.file_path(),
        table.file_io(),
        size,
        ParquetReadOptions::builder().build(),
        metrics.bytes_read_counter(),
        file.key_metadata.as_deref(),
    )
    .await?;
    table.file_io().check_cancellation()?;
    if u64::try_from(opened.1.metadata().file_metadata().num_rows())? != file.record_count() {
        return Err(invalid());
    }
    Ok(opened)
}
