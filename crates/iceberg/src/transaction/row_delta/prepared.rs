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

use crate::io::ArtifactInventory;
use crate::spec::Snapshot;
use crate::table::Table;
use crate::transaction::PreparedRewriteDataFiles;
use crate::{Catalog, Result, TableCommit};

/// Holds a row-delta commit ready for one-shot caller-controlled publication.
///
/// A coordinating adapter must validate requirements and enforce the validated
/// base metadata location atomically with external state. Persist snapshot/base
/// identity and caller-written file paths before submission. Unknown outcomes
/// require recovery; neither publication nor handoff deletes files or retries.
pub struct PreparedRowDelta {
    inner: PreparedRewriteDataFiles,
}

impl PreparedRowDelta {
    pub(super) const fn new(inner: PreparedRewriteDataFiles) -> Self {
        Self { inner }
    }

    /// Returns metadata outputs allocated by preparation attempts.
    #[must_use]
    pub fn artifacts(&self) -> ArtifactInventory {
        self.inner.artifacts()
    }

    /// Returns the prepared snapshot identity and manifest-list location.
    #[must_use]
    pub const fn snapshot(&self) -> &Snapshot {
        self.inner.snapshot()
    }

    /// Returns the validated publication-base metadata location.
    #[must_use]
    pub fn base_metadata_location(&self) -> Option<&str> {
        self.inner.base_metadata_location()
    }

    /// Returns the validated publication-base snapshot identifier.
    #[must_use]
    pub fn base_snapshot_id(&self) -> Option<i64> {
        self.inner.base_snapshot_id()
    }

    /// Checks cancellation and transfers the commit to the caller's coordinator.
    ///
    /// # Errors
    /// Returns `Cancelled` before handoff if cancellation has been requested.
    pub fn try_into_table_commit(self) -> Result<TableCommit> {
        self.inner.try_into_table_commit()
    }

    /// Publishes once through a catalog after checking the prepared base.
    ///
    /// # Errors
    /// Returns a stale-base, cancellation, or catalog error. A submitted catalog
    /// error may have an unknown outcome; retain all files until it is resolved.
    pub async fn publish(self, catalog: &dyn Catalog) -> Result<Table> {
        self.inner.publish(catalog).await
    }
}
