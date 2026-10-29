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

#![forbid(unsafe_code)]
#![deny(warnings, dead_code, unused, missing_docs)]
#![deny(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
#![deny(clippy::all, clippy::pedantic, clippy::nursery)]
#![deny(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::excessive_nesting,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::undocumented_unsafe_blocks,
    clippy::as_conversions,
    clippy::wildcard_imports
)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Shared conservative inventory of newly allocated output paths.
///
/// Paths are registered before storage allocation, so entries may be absent,
/// partial, or complete. Existing input paths are never registered. Retain a
/// clone before execution to inspect failures or dropped futures. This handle
/// is process-local and never authorizes physical deletion.
#[derive(Clone, Debug, Default)]
pub struct ArtifactInventory {
    paths: Arc<Mutex<BTreeSet<String>>>,
    protected: Arc<AtomicBool>,
}

impl ArtifactInventory {
    /// Returns every allocated output path in deterministic order.
    #[must_use]
    pub fn paths(&self) -> Vec<String> {
        self.paths
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    /// Returns whether publication may have made these files reachable.
    #[must_use]
    pub fn publication_protected(&self) -> bool {
        self.protected.load(Ordering::SeqCst)
    }

    pub(super) fn register(&self, path: &str) {
        self.paths
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.to_string());
    }

    pub(crate) fn protect(&self) {
        self.protected.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn allocation_is_registered_before_a_storage_write_failure() -> crate::Result<()> {
        let directory = tempfile::TempDir::new()?;
        let blocked = directory.path().join("blocked");
        std::fs::write(&blocked, b"not a directory")?;
        let path = blocked
            .join("partial.parquet")
            .to_string_lossy()
            .into_owned();
        let inventory = super::ArtifactInventory::default();
        let io = crate::io::FileIO::new_with_fs().track_outputs(inventory.clone());
        if let Ok(output) = io.new_output(&path) {
            assert!(output.writer().await.is_err());
        }
        assert_eq!(inventory.paths(), vec![path]);
        assert!(!inventory.publication_protected());
        Ok(())
    }
}
