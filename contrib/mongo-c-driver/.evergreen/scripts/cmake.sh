#!/usr/bin/env bash

set -eu
. "$(dirname "${BASH_SOURCE[0]}")/find-cmake-latest.sh"

cmake=$(find_cmake_latest)

$cmake "$@"
