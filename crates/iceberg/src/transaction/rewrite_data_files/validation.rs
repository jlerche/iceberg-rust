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

use super::{RewriteDataFilesAction, validate_base};
use crate::spec::Operation;
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

pub(super) async fn validate_publication(base: &Table, table: &Table) -> Result<()> {
    if validate_base(base, table).is_ok() {
        return Ok(());
    }
    validate_configuration(base, table)?;
    validate_append_history(base, table)?;
    let (original, sequences) = RewriteDataFilesAction::new(base).inputs(base).await?;
    let action = RewriteDataFilesAction::new(table);
    let (current, current_sequences) = action.inputs(table).await?;
    let current: std::collections::HashMap<_, _> = current
        .iter()
        .map(|file| (file.file_path(), file))
        .collect();
    if original.iter().any(|file| {
        current.get(file.file_path()).copied() != Some(file)
            || sequences.get(file.file_path()) != current_sequences.get(file.file_path())
    }) {
        return Err(changed());
    }
    Ok(())
}

fn validate_configuration(base: &Table, table: &Table) -> Result<()> {
    let old = base.metadata();
    let fresh = table.metadata();
    if base.readonly()
        || table.readonly()
        || base.identifier() != table.identifier()
        || old.uuid() != fresh.uuid()
        || old.location() != fresh.location()
        || old.format_version() != fresh.format_version()
        || old.current_schema() != fresh.current_schema()
        || old.default_partition_spec_id() != fresh.default_partition_spec_id()
        || old.partition_specs_iter().len() != fresh.partition_specs_iter().len()
        || old
            .partition_specs_iter()
            .any(|spec| fresh.partition_spec_by_id(spec.spec_id()) != Some(spec))
        || old.default_sort_order() != fresh.default_sort_order()
        || old.properties() != fresh.properties()
    {
        return Err(changed());
    }
    Ok(())
}

fn validate_append_history(base: &Table, table: &Table) -> Result<()> {
    let source = base.metadata().current_snapshot().ok_or_else(changed)?;
    let mut current = table.metadata().current_snapshot().ok_or_else(changed)?;
    let mut visited = HashSet::new();
    while current.snapshot_id() != source.snapshot_id() {
        if !visited.insert(current.snapshot_id())
            || current.summary().operation != Operation::Append
        {
            return Err(changed());
        }
        let parent = current.parent_snapshot_id().ok_or_else(changed)?;
        let previous = table
            .metadata()
            .snapshot_by_id(parent)
            .ok_or_else(changed)?;
        if previous.sequence_number() >= current.sequence_number() {
            return Err(changed());
        }
        current = previous;
    }
    if current != source || visited.is_empty() {
        return Err(changed());
    }
    Ok(())
}

fn changed() -> Error {
    Error::new(
        ErrorKind::PreconditionFailed,
        "Compaction requires an unchanged base or a compatible append-only descendant; replan the rewrite",
    )
}
