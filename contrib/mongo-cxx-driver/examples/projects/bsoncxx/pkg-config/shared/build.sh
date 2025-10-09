#!/bin/bash
set -o errexit
set -o pipefail

CXX=${CXX:-c++}
CXX_STANDARD=${CXX_STANDARD:-11}

# Sanity-check that static library macros are not set when building against the shared library.
# Users don't need to include this section in their projects.
( pkg-config --cflags libbsoncxx | grep -v -- -DBSONCXX_STATIC ) \
    || ( echo "Expected BSONCXX_STATIC to not be set" >&2; exit 1 )

rm -rf build/*
cd build
$CXX $CXXFLAGS -Wall -Wextra -Werror -std="c++${CXX_STANDARD}" -c -o hello_bsoncxx.o ../../../hello_bsoncxx.cpp $(pkg-config --cflags libbsoncxx)
$CXX $LDFLAGS -std="c++${CXX_STANDARD}" -o hello_bsoncxx hello_bsoncxx.o $(pkg-config --libs libbsoncxx)
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:../../../../../../build/install/lib
./hello_bsoncxx
