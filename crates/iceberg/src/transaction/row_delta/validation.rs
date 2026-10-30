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

use std::collections::HashSet;

use crate::spec::{DataContentType, DataFile, DataFileFormat, FormatVersion};
use crate::table::Table;
use crate::transaction::snapshot::validate_partition_value;
use crate::writer::base_writer::equality_delete_writer::EqualityDeleteWriterConfig;
use crate::{Error, ErrorKind, Result};

pub(super) fn invalid() -> Error {
    Error::new(ErrorKind::DataInvalid, "Invalid row-delta file descriptor")
}

pub(super) fn validate(
    base: &Table,
    table: &Table,
    data: &[DataFile],
    deletes: &[DataFile],
) -> Result<()> {
    super::super::rewrite_data_files::validate_base(base, table)?;
    if table.metadata().format_version() != FormatVersion::V2 {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Row delta currently requires format v2",
        ));
    }
    if data.is_empty() && deletes.is_empty() {
        return Err(invalid());
    }
    let mut paths = HashSet::new();
    for (files, expected) in [
        (data, DataContentType::Data),
        (deletes, DataContentType::EqualityDeletes),
    ] {
        for file in files {
            validate_file(table, file, expected)?;
            if !paths.insert(file.file_path()) {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

fn validate_file(table: &Table, file: &DataFile, expected: DataContentType) -> Result<()> {
    if file.file_format() != DataFileFormat::Parquet {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Row delta supports only Parquet files",
        ));
    }
    let position = file.content_type() == DataContentType::PositionDeletes;
    if (file.content_type() != expected
        && !(expected == DataContentType::EqualityDeletes && position))
        || file.file_path().is_empty()
        || file.file_size_in_bytes() == 0
        || (!position && file.referenced_data_file.is_some())
        || (position && file.sort_order_id.is_some())
        || file.content_offset.is_some()
        || file.content_size_in_bytes.is_some()
    {
        return Err(invalid());
    }
    i64::try_from(file.record_count())?;
    i64::try_from(file.file_size_in_bytes())?;
    let spec = table
        .metadata()
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(invalid)?;
    validate_partition_value(
        file.partition(),
        &spec.partition_type(table.metadata().current_schema())?,
    )?;
    if file.content_type() == DataContentType::EqualityDeletes {
        EqualityDeleteWriterConfig::new(
            file.equality_ids.clone().ok_or_else(invalid)?,
            table.metadata().current_schema().clone(),
        )?;
    } else if file.equality_ids.is_some() {
        return Err(invalid());
    }
    Ok(())
}
