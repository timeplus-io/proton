#!/bin/bash
set -o errexit
set -o pipefail

CXX=${CXX:-c++}
CXX_STANDARD=${CXX_STANDARD:-11}

# Sanity-check that static library macros are set when building against the static library.  Users
# don't need to include this section in their projects.
( pkg-config --cflags libmongocxx-static | grep -- -DBSONCXX_STATIC ) \
    || ( echo "Expected BSONCXX_STATIC to be set" >&2; exit 1 )
( pkg-config --cflags libmongocxx-static | grep -- -DMONGOCXX_STATIC ) \
    || ( echo "Expected MONGOCXX_STATIC to be set" >&2; exit 1 )

rm -rf build/*
cd build
$CXX $CXXFLAGS -Wall -Wextra -Werror -std="c++${CXX_STANDARD}" -c -o hello_mongocxx.o ../../../hello_mongocxx.cpp $(pkg-config --cflags libmongocxx-static) $PKGCONFIG_EXTRA_OPTS
# TODO: remove `-pthread` once CDRIVER-4776 is resolved.
$CXX $LDFLAGS -pthread -std="c++${CXX_STANDARD}" -o hello_mongocxx hello_mongocxx.o $(pkg-config --libs libmongocxx-static) $PKGCONFIG_EXTRA_OPTS
./hello_mongocxx
