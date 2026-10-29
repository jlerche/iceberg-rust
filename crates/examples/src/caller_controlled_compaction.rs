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

//! Caller-controlled compaction with conflicts and uncertain publication outcomes.

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

#[path = "compaction/demo.rs"]
mod demo;
#[path = "compaction/recovery.rs"]
mod recovery;

use demo::{Demo, missing, rows};
use iceberg::table::Table;
use iceberg::transaction::{RewriteDataFilesResult, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result, TableCommit};
use recovery::{Observation, Receipt};

#[tokio::main]
async fn main() -> Result<()> {
    let demo = Demo::new().await?;
    successful_handoff(&demo).await?;
    definitive_conflict(&demo).await?;
    unknown_outcome(&demo).await?;
    println!(
        "All scenarios passed. Demo files remain at {}",
        demo.warehouse.display()
    );
    Ok(())
}

async fn execute_selected(demo: &Demo, name: &str) -> Result<(Table, RewriteDataFilesResult)> {
    let (base, selected) = demo.seed(name).await?;
    let plan = Transaction::new(&base)
        .rewrite_data_files()
        .select_data_files(selected)
        .with_plan_limits(2, 64 * 1024 * 1024)?
        .plan()
        .await?;
    let inventory = plan.artifacts();
    let result = plan.execute().await?;
    assert_eq!(
        (result.input_files().len(), result.output_files().len()),
        (2, 1)
    );
    assert!(!inventory.publication_protected());
    let fresh = demo.catalog.load_table(base.identifier()).await?;
    assert_eq!(fresh.metadata(), base.metadata());
    Ok((base, result))
}

async fn successful_handoff(demo: &Demo) -> Result<()> {
    let (base, result) = execute_selected(demo, "success").await?;
    let fresh = demo.catalog.load_table(base.identifier()).await?;
    let prepared = result.prepare(&fresh).await?.ok_or_else(missing)?;
    let receipt = Receipt::record(&fresh, &prepared);
    let before = demo.catalog.load_table(base.identifier()).await?;
    assert_eq!(before.metadata(), base.metadata());
    let commit = prepared.try_into_table_commit()?;
    assert!(receipt.artifacts.publication_protected());
    let committed = demo.catalog.update_table(commit).await?;
    assert_eq!(receipt.observe(&committed), Observation::Committed);
    assert_eq!(
        (
            rows(&committed, None).await?,
            rows(&committed, base.metadata().current_snapshot_id()).await?
        ),
        (vec![None, Some(1), Some(1), Some(2), Some(99)], vec![
            None,
            Some(1),
            Some(1),
            Some(2),
            Some(99)
        ])
    );
    receipt.check_artifacts(&committed).await?;
    println!("success: selected inputs compacted; untouched file and history preserved");
    Ok(())
}

async fn definitive_conflict(demo: &Demo) -> Result<()> {
    let (base, result) = execute_selected(demo, "conflict").await?;
    let prepared = result.prepare(&base).await?.ok_or_else(missing)?;
    let rejected = Receipt::record(&base, &prepared);
    let data_paths = demo::data_paths(&result);
    let (appended, _) = demo.append(&base, &[Some(3)]).await?;
    let error = demo
        .catalog
        .update_table(prepared.try_into_table_commit()?)
        .await
        .err()
        .ok_or_else(missing)?;
    // This catalog rejects failed requirements before changing its table pointer.
    if error.kind() != ErrorKind::CatalogCommitConflicts {
        return Err(error);
    }
    let fresh = demo.catalog.load_table(base.identifier()).await?;
    assert_eq!(fresh.metadata(), appended.metadata());
    assert_eq!(rejected.observe(&fresh), Observation::Unknown);
    let prepared = result.prepare(&fresh).await?.ok_or_else(missing)?;
    let receipt = Receipt::record(&fresh, &prepared);
    let committed = demo
        .catalog
        .update_table(prepared.try_into_table_commit()?)
        .await?;
    assert_eq!(
        (demo::data_paths(&result), rows(&committed, None).await?),
        (data_paths, vec![
            None,
            Some(1),
            Some(1),
            Some(2),
            Some(3),
            Some(99)
        ])
    );
    assert_eq!(receipt.observe(&committed), Observation::Committed);
    rejected.check_artifacts(&committed).await?;
    println!(
        "conflict: definitive rejection; refreshed and prepared again using the same data outputs"
    );
    Ok(())
}

async fn lose_success_response(catalog: &dyn Catalog, commit: TableCommit) -> Result<Table> {
    catalog.update_table(commit).await?;
    // Simulates response loss after publication, not a transport fault.
    Err(Error::new(ErrorKind::Unexpected, "Simulated lost success response").with_retryable(true))
}

async fn unknown_outcome(demo: &Demo) -> Result<()> {
    let (base, result) = execute_selected(demo, "unknown").await?;
    let prepared = result.prepare(&base).await?.ok_or_else(missing)?;
    let receipt = Receipt::record(&base, &prepared);
    let snapshots_before = base.metadata().snapshots().len();
    let error = lose_success_response(&demo.catalog, prepared.try_into_table_commit()?)
        .await
        .err()
        .ok_or_else(missing)?;
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(receipt.artifacts.publication_protected());
    assert_eq!(receipt.observe(&base), Observation::Unknown);
    receipt.check_artifacts(&base).await?;
    println!("unknown: stale read cannot establish the outcome; retaining files without retrying");
    let fresh = demo.catalog.load_table(base.identifier()).await?;
    let (advanced, _) = demo.append(&fresh, &[Some(4)]).await?;
    assert_eq!(receipt.observe(&advanced), Observation::Committed);
    assert_eq!(
        (
            advanced.metadata().snapshots().len(),
            rows(&advanced, None).await?
        ),
        (snapshots_before + 2, vec![
            None,
            Some(1),
            Some(1),
            Some(2),
            Some(4),
            Some(99)
        ])
    );
    receipt.check_artifacts(&advanced).await?;
    println!(
        "recovered: exact prepared snapshot is on main's retained ancestry; no second rewrite submitted"
    );
    Ok(())
}
