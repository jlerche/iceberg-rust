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

use super::super::ActionCommit;
use super::validate_base;
use crate::spec::Snapshot;
use crate::table::Table;
use crate::{Catalog, Error, ErrorKind, Result, TableCommit, TableUpdate};

/// Holds a single standard Iceberg commit ready for caller-controlled publication.
///
/// Catalog adapters must validate requirements and publish atomically. An adapter
/// coordinating external metadata must also enforce the base metadata location
/// in that same transaction. This object does not make separate stores atomic.
/// Once submitted, an ambiguous response requires caller recovery; do not retry
/// blindly or delete staged files. Publication never retries automatically.
pub struct PreparedRewriteDataFiles {
    base: Table,
    snapshot: Snapshot,
    commit: TableCommit,
}

impl PreparedRewriteDataFiles {
    pub(in crate::transaction) fn new(base: Table, mut action: ActionCommit) -> Result<Self> {
        let updates = action.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "Prepared mutation lacks a snapshot")
            })?;
        let commit = TableCommit::builder()
            .ident(base.identifier().clone())
            .requirements(action.take_requirements())
            .updates(updates)
            .build();
        Ok(Self {
            base,
            snapshot,
            commit,
        })
    }

    /// Returns the shared staged-output inventory.
    #[must_use]
    pub fn artifacts(&self) -> crate::io::ArtifactInventory {
        self.base.file_io().inventory().unwrap_or_default()
    }

    /// Returns the prepared snapshot identity and manifest-list location.
    #[must_use]
    pub const fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Returns the validated publication-base metadata location for catalog compare-and-swap.
    #[must_use]
    pub fn base_metadata_location(&self) -> Option<&str> {
        self.base.metadata_location()
    }

    /// Returns the validated publication-base snapshot identifier.
    #[must_use]
    pub fn base_snapshot_id(&self) -> Option<i64> {
        self.base.metadata().current_snapshot_id()
    }

    /// Checks cancellation and transfers the commit to a caller-controlled coordinator.
    ///
    /// # Errors
    /// Returns `Cancelled` before handoff if the token has been cancelled.
    pub fn try_into_table_commit(self) -> Result<TableCommit> {
        self.base.file_io().check_cancellation()?;
        Ok(self.into_table_commit())
    }

    /// Transfers the standard commit to the caller's catalog or atomic coordinator.
    #[must_use]
    /// This unchecked handoff delegates cancellation checks to the caller.
    pub fn into_table_commit(self) -> TableCommit {
        self.artifacts().protect();
        self.commit
    }

    /// Submits the prepared commit once through a catalog after checking its base.
    ///
    /// # Errors
    /// Returns a stale-base or catalog error. A catalog error after submission
    /// may have an unknown outcome; no staged artifacts are deleted.
    pub async fn publish(self, catalog: &dyn Catalog) -> Result<Table> {
        self.base.file_io().check_cancellation()?;
        let fresh = catalog.load_table(self.base.identifier()).await?;
        self.base.file_io().check_cancellation()?;
        validate_base(&self.base, &fresh)?;
        self.base.file_io().check_cancellation()?;
        self.artifacts().protect();
        catalog.update_table(self.commit).await
    }
}
