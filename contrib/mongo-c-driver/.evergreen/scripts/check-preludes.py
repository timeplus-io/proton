#!/usr/bin/env python3
#
# Copyright 2019-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check that public libbson/libmongoc headers all include the prelude line.
"""
import sys
from pathlib import Path

if len(sys.argv) != 2:
    print("Usage: python check-preludes.py <mongo-c-driver directory>")
    sys.exit(1)

MONGOC_PREFIX = Path("src/libmongoc/src/mongoc")
BSON_PREFIX = Path("src/libbson/src/bson")
COMMON_PREFIX = Path("src/common")

checks = [
    {
        "name": "libmongoc",
        "headers": list(MONGOC_PREFIX.glob("mongoc-*.h")),
        "exclusions": [
            MONGOC_PREFIX / "mongoc-prelude.h",
            MONGOC_PREFIX / "mongoc.h",
        ],
        "include": '#include "mongoc-prelude.h"',
    },
    {
        "name": "libbson",
        "headers": list(BSON_PREFIX.glob("*.h")),
        "exclusions": [
            BSON_PREFIX / "bson-dsl.h",
            BSON_PREFIX / "bson-prelude.h",
            BSON_PREFIX / "bson.h",
        ],
        "include": "#include <bson/bson-prelude.h>",
    },
    {
        "name": "common",
        "headers": list(COMMON_PREFIX.glob("*.h")),
        "exclusions": [COMMON_PREFIX / "common-prelude.h"],
        "include": '#include "common-prelude.h"',
    },
]

for check in checks:
    NAME = check["name"]
    print(f"Checking headers for {NAME}")
    assert len(check["headers"]) > 0
    for header in check["headers"]:
        if header in check["exclusions"]:
            continue
        lines = Path(header).read_text(encoding="utf-8").splitlines()
        if check["include"] not in lines:
            print(f"{header} did not include prelude")
            sys.exit(1)

print("All checks passed")
