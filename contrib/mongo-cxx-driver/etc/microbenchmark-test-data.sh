#!/usr/bin/env bash

# This script is used to fetch the latest test data for the microbenchmark suite. It puts the data in the
# directory $reporoot/data/benchmark. It should be run from the root of the repository.

set -o errexit
set -o nounset

if [ ! -d ".git" ]; then
    echo "$0: This script must be run from the root of the repository" >&2
    exit 1
fi

tmpdir=`perl -MFile::Temp=tempdir -wle 'print tempdir(TMPDIR => 1, CLEANUP => 0)'`
curl -sL https://github.com/mongodb-labs/driver-performance-test-data/archive/master.zip -o "$tmpdir/data.zip"
unzip -d "$tmpdir" "$tmpdir/data.zip" > /dev/null
mkdir -p data/benchmark

pushd "$tmpdir/driver-performance-test-data-master" > /dev/null

tar xf extended_bson.tgz
tar xf parallel.tgz
tar xf single_and_multi_document.tgz

rm extended_bson.tgz
rm parallel.tgz
rm single_and_multi_document.tgz
rm README.md

popd > /dev/null
rsync -ah "$tmpdir/driver-performance-test-data-master/" data/benchmark
rm -rf "$tmpdir"