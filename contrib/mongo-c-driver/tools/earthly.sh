#!/usr/bin/env bash

. "$(dirname "${BASH_SOURCE[0]}")/use.sh" paths platform

set -euo pipefail

: "${EARTHLY_VERSION:=0.8.3}"

# Calc the arch of the executable we want
case "$ARCHNAME" in
    x64)
        arch=amd64
        ;;
    arm64)
        arch=arm64
        ;;
    *)
        echo "Unsupported architecture for automatic Earthly download: $HOSTTYPE" 1>&1
        exit 99
        ;;
esac

# The location where the Earthly executable will live
cache_dir="$USER_CACHES_DIR/earthly-sh/$EARTHLY_VERSION"
mkdir -p "$cache_dir"

exe_filename="earthly-$OS_FAMILY-$arch$EXE_SUFFIX"
EARTHLY_EXE="$cache_dir/$exe_filename"

# Download if it isn't already present
if ! is-file "$EARTHLY_EXE"; then
    echo "Downloading $exe_filename $EARTHLY_VERSION"
    url="https://github.com/earthly/earthly/releases/download/v$EARTHLY_VERSION/$exe_filename"
    curl --retry 5 -LsS --max-time 120 --fail "$url" --output "$EARTHLY_EXE"
    chmod a+x "$EARTHLY_EXE"
fi

run-earthly() {
    "$EARTHLY_EXE" "$@"
}

if is-main; then
    run-earthly "$@"
fi
