# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Read only the Rust acceptance fixtures through Spark's Iceberg reader."""

import json
import re
import sys
from collections import Counter

from pyspark.sql import SparkSession


def verify(spark, case):
    namespace = case["namespace"]
    table = case["table"]
    if not re.fullmatch(r"rewrite_[0-9a-f]{32}", namespace) or table != "rows":
        raise ValueError("Expected an isolated Rust acceptance fixture")
    identifier = f"rest.{namespace}.{table}"
    references = {
        row.name: row.snapshot_id
        for row in spark.read.format("iceberg").load(f"{identifier}.refs").collect()
    }
    assert case["head_snapshot_id"] is not None
    assert references["main"] == case["head_snapshot_id"], (identifier, references)
    operations = {
        row.snapshot_id: row.operation
        for row in spark.read.format("iceberg").load(f"{identifier}.snapshots").collect()
    }
    for check in case["checks"]:
        reader = spark.read.format("iceberg")
        if check["snapshot_id"] is not None:
            reader = reader.option("snapshot-id", str(check["snapshot_id"]))
            assert operations[check["snapshot_id"]] == check["operation"], check
        actual = Counter(tuple(row) for row in reader.load(identifier).select("id", "payload").collect())
        expected = Counter(tuple(row) for row in check["rows"])
        assert actual == expected, f"{identifier}/{check['label']}: {actual!r} != {expected!r}"
        print(f"PASS {identifier}/{check['label']}: {sum(actual.values())} rows", flush=True)


def main():
    cases = json.loads(sys.argv[1])
    spark = SparkSession.builder.appName("Rust Iceberg mutation acceptance").getOrCreate()
    try:
        print(f"Spark {spark.version}; {spark._jvm.org.apache.iceberg.IcebergBuild.fullVersion()}", flush=True)
        for case in cases:
            verify(spark, case)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
