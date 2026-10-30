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

use std::path::Path;
use std::process::Command;

use iceberg::{Error, ErrorKind, Result};

use super::cases::{Case, Expectations};
use super::fixture::run;

#[test]
#[ignore = "requires REST/RustFS plus the built Spark Compose image; see implementation plan"]
fn spark_reads_rust_row_delta_and_compaction_snapshots() -> Result<()> {
    run(async {
        let cases = vec![Case::equality().await?, Case::positions().await?];
        let expected = cases.iter().map(Case::expectations).collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || verify(&expected))
            .await
            .map_err(|error| {
                Error::new(ErrorKind::Unexpected, "Spark verifier task failed").with_source(error)
            })??;
        for case in cases {
            case.fixture.cleanup().await?;
        }
        Ok(())
    })
}

fn verify(expected: &[Expectations]) -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script = root
        .join("dev/spark/verify_mutation_snapshots.py")
        .canonicalize()?;
    let compose = root.join("dev/docker-compose.yaml").canonicalize()?;
    let project = std::env::var("ICEBERG_ACCEPTANCE_COMPOSE_PROJECT")
        .unwrap_or_else(|_| "iceberg-acceptance".to_string());
    let payload = serde_json::to_string(expected)?;
    let output = Command::new("docker")
        .args(["compose", "-p", &project, "-f"])
        .arg(compose)
        .args(["run", "--rm", "--no-deps", "-T", "--volume"])
        .arg(format!(
            "{}:/opt/verify_mutation_snapshots.py:ro",
            script.display()
        ))
        .args([
            "spark-iceberg",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "--conf",
            "spark.ui.enabled=false",
            "--conf",
            "spark.sql.shuffle.partitions=1",
            "--conf",
            "spark.sql.catalog.rest.s3.path-style-access=true",
            "/opt/verify_mutation_snapshots.py",
        ])
        .arg(payload)
        .output()?;
    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            format!(
                "Spark verification failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }
    println!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}
