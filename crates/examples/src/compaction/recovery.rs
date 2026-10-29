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

use iceberg::io::ArtifactInventory;
use iceberg::spec::Snapshot;
use iceberg::table::Table;
use iceberg::transaction::PreparedRewriteDataFiles;
use iceberg::{Result, TableIdent};

#[derive(Debug, PartialEq, Eq)]
pub enum Observation {
    Committed,
    Unknown,
}

pub struct Receipt {
    identifier: TableIdent,
    table_uuid: uuid::Uuid,
    snapshot: Snapshot,
    pub artifacts: ArtifactInventory,
}

impl Receipt {
    pub fn record(table: &Table, prepared: &PreparedRewriteDataFiles) -> Self {
        println!(
            "record before handoff: table={}, uuid={}, base={:?}, metadata={:?}, snapshot={}, manifest-list={}, artifacts={:?}",
            table.identifier(),
            table.metadata().uuid(),
            prepared.base_snapshot_id(),
            prepared.base_metadata_location(),
            prepared.snapshot().snapshot_id(),
            prepared.snapshot().manifest_list(),
            prepared.artifacts().paths()
        );
        Self {
            identifier: table.identifier().clone(),
            table_uuid: table.metadata().uuid(),
            snapshot: prepared.snapshot().clone(),
            artifacts: prepared.artifacts(),
        }
    }

    pub fn observe(&self, table: &Table) -> Observation {
        if table.identifier() != &self.identifier || table.metadata().uuid() != self.table_uuid {
            return Observation::Unknown;
        }
        let mut current = table.metadata().current_snapshot();
        let mut visited = HashSet::new();
        while let Some(snapshot) = current {
            if !visited.insert(snapshot.snapshot_id()) {
                return Observation::Unknown;
            }
            if snapshot.as_ref() == &self.snapshot {
                return Observation::Committed;
            }
            current = snapshot
                .parent_snapshot_id()
                .and_then(|id| table.metadata().snapshot_by_id(id));
        }
        // Absence can mean a stale read, an in-flight request, or expired history.
        Observation::Unknown
    }

    pub async fn check_artifacts(&self, table: &Table) -> Result<()> {
        for path in self.artifacts.paths() {
            assert!(table.file_io().new_input(path)?.exists().await?);
        }
        Ok(())
    }
}
