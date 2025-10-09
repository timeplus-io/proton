#!/usr/bin/env bash
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#   LINK_STATIC              Whether to statically link to libbson
#   BUILD_SAMPLE_WITH_CMAKE  Link program w/ CMake. Default: use pkg-config.
#   BUILD_SAMPLE_WITH_CMAKE_DEPRECATED  If BUILD_SAMPLE_WITH_CMAKE is set, then use deprecated CMake scripts instead.


echo "LINK_STATIC=$LINK_STATIC BUILD_SAMPLE_WITH_CMAKE=$BUILD_SAMPLE_WITH_CMAKE BUILD_SAMPLE_WITH_CMAKE_DEPRECATED=$BUILD_SAMPLE_WITH_CMAKE_DEPRECATED"

DIR=$(dirname $0)
. $DIR/find-cmake-latest.sh
CMAKE=$(find_cmake_latest)
. $DIR/check-symlink.sh

# Get the kernel name, lowercased
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
echo "OS: $OS"

if [ "$OS" = "darwin" ]; then
  SO=dylib
  LIB_SO=libbson-1.0.0.dylib
  LDD="otool -L"
else
  SO=so
  LIB_SO=libbson-1.0.so.0.0.0
  LDD=ldd
fi

SRCROOT=`pwd`
SCRATCH_DIR=$(pwd)/.scratch
rm -rf "$SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR"
cp -r -- "$SRCROOT"/* "$SCRATCH_DIR"

BUILD_DIR=$SCRATCH_DIR/build-dir
rm -rf $BUILD_DIR
mkdir $BUILD_DIR

INSTALL_DIR=$SCRATCH_DIR/install-dir
rm -rf $INSTALL_DIR
mkdir -p $INSTALL_DIR

cd $BUILD_DIR

# Allow reuse of ccache compilation results between different build directories.
export CCACHE_BASEDIR CCACHE_NOHASHDIR
CCACHE_BASEDIR="$SCRATCH_DIR"
CCACHE_NOHASHDIR=1

if [ "$LINK_STATIC" ]; then
  # Our CMake system builds shared and static by default.
  $CMAKE -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DENABLE_TESTS=OFF "$SCRATCH_DIR"
  $CMAKE --build . --parallel
  $CMAKE --build . --parallel --target install
else
  $CMAKE -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DENABLE_TESTS=OFF -DENABLE_STATIC=OFF "$SCRATCH_DIR"
  $CMAKE --build . --parallel
  $CMAKE --build . --parallel --target install

  set +o xtrace

  if test -f $INSTALL_DIR/lib/libbson-static-1.0.a; then
    echo "libbson-static-1.0.a shouldn't have been installed"
    exit 1
  fi
  if test -f $INSTALL_DIR/lib/libbson-1.0.a; then
    echo "libbson-1.0.a shouldn't have been installed"
    exit 1
  fi
  if test -f $INSTALL_DIR/lib/pkgconfig/libbson-static-1.0.pc; then
    echo "libbson-static-1.0.pc shouldn't have been installed"
    exit 1
  fi

fi

# Revert ccache options, they no longer apply.
unset CCACHE_BASEDIR CCACHE_NOHASHDIR

ls -l $INSTALL_DIR/lib

set +o xtrace

# Check on Linux that libbson is installed into lib/ like:
# libbson-1.0.so -> libbson-1.0.so.0
# libbson-1.0.so.0 -> libbson-1.0.so.0.0.0
# libbson-1.0.so.0.0.0
if [ "$OS" != "darwin" ]; then
  # From check-symlink.sh
  check_symlink libbson-1.0.so libbson-1.0.so.0
  check_symlink libbson-1.0.so.0 libbson-1.0.so.0.0.0
  SONAME=$(objdump -p $INSTALL_DIR/lib/$LIB_SO|grep SONAME|awk '{print $2}')
  EXPECTED_SONAME="libbson-1.0.so.0"
  if [ "$SONAME" != "$EXPECTED_SONAME" ]; then
    echo "SONAME should be $EXPECTED_SONAME, not $SONAME"
    exit 1
  else
    echo "library name check ok, SONAME=$SONAME"
  fi
else
  # Just test that the shared lib was installed.
  if test ! -f $INSTALL_DIR/lib/$LIB_SO; then
    echo "$LIB_SO missing!"
    exit 1
  else
    echo "$LIB_SO check ok"
  fi
fi

if test ! -f $INSTALL_DIR/lib/pkgconfig/libbson-1.0.pc; then
  echo "libbson-1.0.pc missing!"
  exit 1
else
  echo "libbson-1.0.pc check ok"
fi
if test ! -f $INSTALL_DIR/lib/cmake/bson-1.0/bson-1.0-config.cmake; then
  echo "bson-1.0-config.cmake missing!"
  exit 1
else
  echo "bson-1.0-config.cmake check ok"
fi
if test ! -f $INSTALL_DIR/lib/cmake/bson-1.0/bson-1.0-config-version.cmake; then
  echo "bson-1.0-config-version.cmake missing!"
  exit 1
else
  echo "bson-1.0-config-version.cmake check ok"
fi
if test ! -f $INSTALL_DIR/lib/cmake/bson-1.0/bson-targets.cmake; then
  echo "bson-targets.cmake missing!"
  exit 1
else
  echo "bson-targets.cmake check ok"
fi

if [ "$LINK_STATIC" ]; then
  if test ! -f $INSTALL_DIR/lib/libbson-static-1.0.a; then
    echo "libbson-static-1.0.a missing!"
    exit 1
  else
    echo "libbson-static-1.0.a check ok"
  fi
  if test ! -f $INSTALL_DIR/lib/pkgconfig/libbson-static-1.0.pc; then
    echo "libbson-static-1.0.pc missing!"
    exit 1
  else
    echo "libbson-static-1.0.pc check ok"
  fi
fi

cd $SRCROOT

if [ "$BUILD_SAMPLE_WITH_CMAKE" ]; then
  # Test our CMake package config file with CMake's find_package command.
  if [ "$BUILD_SAMPLE_WITH_CMAKE_DEPRECATED" ]; then
    EXAMPLE_DIR=$SRCROOT/src/libbson/examples/cmake-deprecated/find_package
  else
    EXAMPLE_DIR=$SRCROOT/src/libbson/examples/cmake/find_package
  fi

  if [ "$LINK_STATIC" ]; then
    EXAMPLE_DIR="${EXAMPLE_DIR}_static"
  fi

  cd $EXAMPLE_DIR
  $CMAKE -DCMAKE_PREFIX_PATH=$INSTALL_DIR/lib/cmake .
  $CMAKE --build . --parallel
else
  # Test our pkg-config file.
  export PKG_CONFIG_PATH=$INSTALL_DIR/lib/pkgconfig
  cd $SRCROOT/src/libbson/examples

  if [ "$LINK_STATIC" ]; then
    echo "pkg-config output:"
    echo $(pkg-config --libs --cflags libbson-static-1.0)
    sh compile-with-pkg-config-static.sh
  else
    echo "pkg-config output:"
    echo $(pkg-config --libs --cflags libbson-1.0)
    sh compile-with-pkg-config.sh
  fi
fi

if [ ! "$LINK_STATIC" ]; then
  if [ "$OS" = "darwin" ]; then
    export DYLD_LIBRARY_PATH=$INSTALL_DIR/lib
  else
    export LD_LIBRARY_PATH=$INSTALL_DIR/lib
  fi
fi

echo "ldd hello_bson:"
$LDD hello_bson

./hello_bson
