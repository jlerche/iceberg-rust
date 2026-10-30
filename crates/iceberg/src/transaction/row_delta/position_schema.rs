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

use arrow_schema::DataType;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::{ConvertedType, LogicalType, Repetition, Type};

use crate::metadata_columns::{
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use crate::{Error, ErrorKind, Result};

pub(super) fn validate(metadata: &ArrowReaderMetadata) -> Result<()> {
    let physical = metadata
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields();
    let arrow = metadata.schema().fields();
    if physical.len() != 2 || arrow.len() != 2 {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Position publication requires exactly file_path and pos columns",
        ));
    }
    let expected = [
        (
            "file_path",
            RESERVED_FIELD_ID_DELETE_FILE_PATH,
            Type::BYTE_ARRAY,
            DataType::Utf8,
        ),
        (
            "pos",
            RESERVED_FIELD_ID_DELETE_FILE_POS,
            Type::INT64,
            DataType::Int64,
        ),
    ];
    for ((physical, arrow), (name, id, physical_type, arrow_type)) in
        physical.iter().zip(arrow).zip(expected)
    {
        let info = physical.get_basic_info();
        if !physical.is_primitive()
            || !info.has_id()
            || info.id() != id
            || info.name() != name
            || !info.has_repetition()
            || info.repetition() != Repetition::REQUIRED
            || (id == RESERVED_FIELD_ID_DELETE_FILE_PATH
                && info.logical_type_ref() != Some(&LogicalType::String)
                && info.converted_type() != ConvertedType::UTF8)
            || physical.get_physical_type() != physical_type
            || arrow.name() != name
            || arrow.is_nullable()
            || arrow.data_type() != &arrow_type
            || arrow
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|value| value.parse::<i32>().ok())
                != Some(id)
        {
            return Err(super::validation::invalid());
        }
    }
    Ok(())
}
