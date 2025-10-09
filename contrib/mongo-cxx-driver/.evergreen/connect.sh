#!/bin/bash

set -o errexit
set -o pipefail

LIB_DIR=${LIB_DIR:-"lib"}
BUILD_TYPE=${BUILD_TYPE:-"Debug"}

print_usage_and_exit () {
    echo "Error: $1"
    cat << EOF
    connect.sh runs the connect example with a MongoDB URI.

    Example usage:
    MONGOC_INSTALL_PREFIX=/Users/kevin.albertson/install/mongo-c-driver-1.18.0 \\
    MONGOCXX_INSTALL_PREFIX=/Users/kevin.albertson/install/mongo-cxx-driver-dev \\
    LIB_DIR="lib" \\
    BUILD_TYPE="Debug" \\
    BUILD_DIR=$(pwd)/cmake-build \\
    URI="mongodb://localhost:27017/?" \\
        ./.evergreen/connect.sh
EOF
    exit 1;
}

if [ -z "$URI" ]; then
    print_usage_and_exit "URI is a required environment variable."
fi
if [ -z "$MONGOC_INSTALL_PREFIX" ]; then
    print_usage_and_exit "MONGOC_INSTALL_PREFIX is a required environment variable."
fi
if [ -z "$MONGOCXX_INSTALL_PREFIX" ]; then
    print_usage_and_exit "MONGOCXX_INSTALL_PREFIX is a required environment variable."
fi
if [ -z "$BUILD_DIR" ]; then
    print_usage_and_exit "BUILD_DIR is a required environment variable."
fi

# Use PATH / LD_LIBRARY_PATH / DYLD_LIBRARY_PATH to inform the tests where to find
# mongoc library dependencies on Windows / Linux / Mac OS, respectively.
export PATH=$PATH:$MONGOC_INSTALL_PREFIX/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MONGOC_INSTALL_PREFIX/$LIB_DIR/
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$MONGOC_INSTALL_PREFIX/$LIB_DIR/

# Windows also needs to be informed where to find mongocxx library dependencies.
export PATH=$PATH:$BUILD_DIR/src/bsoncxx/$BUILD_TYPE
export PATH=$PATH:$BUILD_DIR/src/mongocxx/$BUILD_TYPE
export PATH=$PATH:$MONGOCXX_INSTALL_PREFIX/bin

if [ "Windows_NT" == "$OS" ]; then
    $BUILD_DIR/examples/mongocxx/$BUILD_TYPE/connect.exe "$URI"
else
    $BUILD_DIR/examples/mongocxx/connect "$URI"
fi
