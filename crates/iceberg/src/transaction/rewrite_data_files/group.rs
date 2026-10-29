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

use futures::TryStreamExt;

use super::row_count;
use crate::cow_rewrite::writer::build_replacement_writer;
use crate::scan::FileScanTaskStream;
use crate::spec::{DataFile, PartitionKey, Struct};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Describes a rewrite group sharing one partition spec and partition value.
pub struct RewriteDataFilesGroup {
    pub(super) files: Vec<DataFile>,
    partition: PartitionKey,
    pub(super) sequence_number: i64,
}

impl RewriteDataFilesGroup {
    /// Returns the source descriptors in this partition group.
    #[must_use]
    pub fn input_files(&self) -> &[DataFile] {
        &self.files
    }

    /// Returns the group's partition spec identifier.
    #[must_use]
    pub fn partition_spec_id(&self) -> i32 {
        self.partition.spec().spec_id()
    }

    /// Returns the group's exact partition value, including null fields.
    #[must_use]
    pub fn partition(&self) -> &Struct {
        self.partition.data()
    }

    /// Returns the minimum input data sequence number inherited by outputs.
    #[must_use]
    pub const fn data_sequence_number(&self) -> i64 {
        self.sequence_number
    }

    pub(super) async fn execute(
        &self,
        table: &Table,
        tasks: FileScanTaskStream,
    ) -> Result<Vec<DataFile>> {
        let expected_rows = row_count(&self.files)?;
        let mut batches = table
            .reader_builder()
            .with_data_file_concurrency_limit(1)
            .build()
            .read(tasks)?
            .stream();
        let mut writer = build_replacement_writer(
            table,
            table.metadata().current_schema().clone(),
            Some(self.partition.clone()),
        )
        .await?;
        let mut rows = 0_u64;
        while let Some(batch) = batches.try_next().await? {
            rows = rows
                .checked_add(u64::try_from(batch.num_rows())?)
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Compaction row count overflow")
                })?;
            if batch.num_rows() > 0 {
                writer.write(batch).await?;
            }
        }
        let outputs = writer.close().await?;
        if rows != expected_rows || row_count(&outputs)? != expected_rows {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Compaction row count does not match source descriptors",
            ));
        }
        Ok(outputs)
    }
}

pub(super) fn group_files(
    table: &Table,
    files: &[DataFile],
    sequences: &HashMap<String, i64>,
) -> Result<Vec<RewriteDataFilesGroup>> {
    let mut indices = HashMap::new();
    let mut groups: Vec<RewriteDataFilesGroup> = vec![];
    for file in files {
        let key = (file.partition_spec_id, file.partition().clone());
        let sequence = *sequences.get(file.file_path()).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Missing compaction input sequence number",
            )
        })?;
        if let Some(index) = indices.get(&key).copied() {
            let group: &mut RewriteDataFilesGroup = &mut groups[index];
            group.files.push(file.clone());
            group.sequence_number = group.sequence_number.min(sequence);
        } else {
            let spec = table
                .metadata()
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Unknown compaction partition spec")
                })?;
            let partition = PartitionKey::new(
                spec.as_ref().clone(),
                table.metadata().current_schema().clone(),
                file.partition().clone(),
            );
            indices.insert(key, groups.len());
            groups.push(RewriteDataFilesGroup {
                files: vec![file.clone()],
                partition,
                sequence_number: sequence,
            });
        }
    }
    Ok(groups)
}

impl RewriteDataFilesGroup {
    fn empty_group(&self) -> Self {
        Self {
            files: vec![],
            partition: self.partition.clone(),
            sequence_number: i64::MAX,
        }
    }

    pub(super) fn split(
        &self,
        max_files: usize,
        max_bytes: u64,
        sequences: &HashMap<String, i64>,
    ) -> Result<Vec<Self>> {
        let mut groups = vec![];
        let mut group = self.empty_group();
        let mut bytes = 0_u64;
        for file in &self.files {
            let size = file.file_size_in_bytes();
            if size > max_bytes {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Input file exceeds rewrite byte limit",
                ));
            }
            if !group.files.is_empty()
                && (group.files.len() == max_files || size > max_bytes - bytes)
            {
                groups.push(group);
                group = self.empty_group();
                bytes = 0;
            }
            if group.files.is_empty() {
                group.sequence_number = i64::MAX;
            }
            group.sequence_number =
                group
                    .sequence_number
                    .min(*sequences.get(file.file_path()).ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Missing input sequence number")
                    })?);
            group.files.push(file.clone());
            bytes += size;
        }
        if !group.files.is_empty() {
            groups.push(group);
        }
        Ok(groups)
    }
}
