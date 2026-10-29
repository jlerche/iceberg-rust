#!/usr/bin/env bash
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

set -euo pipefail
cd "$(dirname "$0")/.."
mapfile -t fork_rust_modules < dev/fork-lints/modules.txt
python3 -m unittest discover -s dev -p 'test_file_length.py'
python3 dev/check_file_length.py "${fork_rust_modules[@]}"
cargo fmt --all -- --check
# Module-level denies re-enable these threshold lints in fork-owned modules.
CLIPPY_CONF_DIR="$PWD/dev/fork-lints" cargo clippy --locked -p iceberg --all-targets --all-features -- \
    -D warnings -A clippy::too_many_arguments -A clippy::type_complexity -A clippy::excessive_nesting
