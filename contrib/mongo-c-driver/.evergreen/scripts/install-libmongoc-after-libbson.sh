#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Install libbson only.
EXTRA_CONFIGURE_FLAGS="-DENABLE_MONGOC=OFF" \
  ./.evergreen/scripts/compile.sh

# Install libmongoc using the system installed libbson.
EXTRA_CONFIGURE_FLAGS="-DENABLE_MONGOC=ON -DUSE_SYSTEM_LIBBSON=ON" \
  ./.evergreen/scripts/compile.sh
