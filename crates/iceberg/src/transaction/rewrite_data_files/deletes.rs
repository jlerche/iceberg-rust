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

use super::super::snapshot::{current_manifests, validate_partition_value};
use crate::spec::{
    DataContentType, DataFileFormat, ManifestContentType, ManifestEntry, ManifestStatus,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

pub(super) fn validate_entry(
    table: &Table,
    entry: &ManifestEntry,
    content: ManifestContentType,
    paths: &mut HashSet<String>,
) -> Result<i64> {
    let file = entry.data_file();
    if content == ManifestContentType::Deletes
        && (file.file_format() != DataFileFormat::Parquet
            || !matches!(
                file.content_type(),
                DataContentType::EqualityDeletes | DataContentType::PositionDeletes
            ))
    {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Compaction supports only Parquet equality and position delete files",
        ));
    }
    if content == ManifestContentType::Data && file.content_type() != DataContentType::Data {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Data manifest contains a delete file",
        ));
    }
    let spec = table
        .metadata()
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Unknown compaction partition spec"))?;
    validate_partition_value(
        file.partition(),
        &spec.partition_type(table.metadata().current_schema())?,
    )?;
    if !paths.insert(entry.file_path().to_string()) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Duplicate live compaction input path",
        ));
    }
    entry
        .sequence_number()
        .filter(|number| *number >= 0 && *number <= table.metadata().last_sequence_number())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Compaction input lacks a valid data sequence number",
            )
        })
}

pub(super) async fn inventory(table: &Table) -> Result<HashMap<String, ManifestEntry>> {
    let mut files = HashMap::new();
    for descriptor in current_manifests(table)
        .await?
        .into_iter()
        .filter(|descriptor| descriptor.content == ManifestContentType::Deletes)
    {
        table.file_io().check_cancellation()?;
        let manifest = table.object_cache().get_manifest(&descriptor).await?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let mut normalized = entry.as_ref().clone();
            normalized.status = ManifestStatus::Existing;
            if files
                .insert(entry.file_path().to_string(), normalized)
                .is_some()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Duplicate live compaction delete path",
                ));
            }
        }
    }
    Ok(files)
}

pub(super) async fn has_live(table: &Table) -> Result<bool> {
    table.file_io().check_cancellation()?;
    for descriptor in current_manifests(table)
        .await?
        .into_iter()
        .filter(|descriptor| descriptor.content == ManifestContentType::Deletes)
    {
        table.file_io().check_cancellation()?;
        if table
            .object_cache()
            .get_manifest(&descriptor)
            .await?
            .entries()
            .iter()
            .any(|entry| entry.is_alive())
        {
            return Ok(true);
        }
    }
    Ok(false)
}
