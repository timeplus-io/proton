#!/usr/bin/env bash
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#   LINK_STATIC                Whether to statically link to libmongoc
#   BUILD_SAMPLE_WITH_CMAKE    Link program w/ CMake. Default: use pkg-config.
#   BUILD_SAMPLE_WITH_CMAKE_DEPRECATED  If BUILD_SAMPLE_WITH_CMAKE is set, then use deprecated CMake scripts instead.
#   ENABLE_SSL                 Set -DENABLE_SSL
#   ENABLE_SNAPPY              Set -DENABLE_SNAPPY
#   CMAKE                      Path to cmake executable.


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
  LIB_SO=libmongoc-1.0.0.dylib
  LDD="otool -L"
else
  SO=so
  LIB_SO=libmongoc-1.0.so.0
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

if [ "$ENABLE_SNAPPY" ]; then
  SNAPPY_CMAKE_OPTION="-DENABLE_SNAPPY=ON"
else
  SNAPPY_CMAKE_OPTION="-DENABLE_SNAPPY=OFF"
fi

if [ "$ENABLE_SSL" ]; then
  if [ "$OS" = "darwin" ]; then
     SSL_CMAKE_OPTION="-DENABLE_SSL:BOOL=DARWIN"
  else
     SSL_CMAKE_OPTION="-DENABLE_SSL:BOOL=OPENSSL"
  fi
else
  SSL_CMAKE_OPTION="-DENABLE_SSL:BOOL=OFF"
fi


if [ "$LINK_STATIC" ]; then
  STATIC_CMAKE_OPTION="-DENABLE_STATIC=ON -DENABLE_TESTS=ON"
else
  STATIC_CMAKE_OPTION="-DENABLE_STATIC=OFF -DENABLE_TESTS=OFF"
fi

ZSTD="AUTO"

# Allow reuse of ccache compilation results between different build directories.
export CCACHE_BASEDIR CCACHE_NOHASHDIR
CCACHE_BASEDIR="$SCRATCH_DIR"
CCACHE_NOHASHDIR=1

$CMAKE -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DCMAKE_PREFIX_PATH=$INSTALL_DIR/lib/cmake $SSL_CMAKE_OPTION $SNAPPY_CMAKE_OPTION $STATIC_CMAKE_OPTION -DENABLE_ZSTD=$ZSTD "$SCRATCH_DIR"
$CMAKE --build . --parallel
$CMAKE --build . --parallel --target install

# Revert ccache options, they no longer apply.
unset CCACHE_BASEDIR CCACHE_NOHASHDIR

ls -l $INSTALL_DIR/lib

set +o xtrace

# Check on Linux that libmongoc is installed into lib/ like:
# libmongoc-1.0.so -> libmongoc-1.0.so.0
# libmongoc-1.0.so.0 -> libmongoc-1.0.so.0.0.0
# libmongoc-1.0.so.0.0.0
if [ "$OS" != "darwin" ]; then
  # From check-symlink.sh
  check_symlink libmongoc-1.0.so libmongoc-1.0.so.0
  check_symlink libmongoc-1.0.so.0 libmongoc-1.0.so.0.0.0
  SONAME=$(objdump -p $INSTALL_DIR/lib/$LIB_SO|grep SONAME|awk '{print $2}')
  EXPECTED_SONAME="libmongoc-1.0.so.0"
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


if test ! -f $INSTALL_DIR/lib/pkgconfig/libmongoc-1.0.pc; then
  echo "libmongoc-1.0.pc missing!"
  exit 1
else
  echo "libmongoc-1.0.pc check ok"
fi
if test ! -f $INSTALL_DIR/lib/cmake/mongoc-1.0/mongoc-1.0-config.cmake; then
  echo "mongoc-1.0-config.cmake missing!"
  exit 1
else
  echo "mongoc-1.0-config.cmake check ok"
fi
if test ! -f $INSTALL_DIR/lib/cmake/mongoc-1.0/mongoc-1.0-config-version.cmake; then
  echo "mongoc-1.0-config-version.cmake missing!"
  exit 1
else
  echo "mongoc-1.0-config-version.cmake check ok"
fi
if test ! -f $INSTALL_DIR/lib/cmake/mongoc-1.0/mongoc-targets.cmake; then
  echo "mongoc-targets.cmake missing!"
  exit 1
else
  echo "mongoc-targets.cmake check ok"
fi


if [ "$LINK_STATIC" ]; then
  if test ! -f $INSTALL_DIR/lib/libmongoc-static-1.0.a; then
    echo "libmongoc-static-1.0.a missing!"
    exit 1
  else
    echo "libmongoc-static-1.0.a check ok"
  fi
  if test ! -f $INSTALL_DIR/lib/pkgconfig/libmongoc-static-1.0.pc; then
    echo "libmongoc-static-1.0.pc missing!"
    exit 1
  else
    echo "libmongoc-static-1.0.pc check ok"
  fi
else
  if test -f $INSTALL_DIR/lib/libmongoc-static-1.0.a; then
    echo "libmongoc-static-1.0.a shouldn't have been installed"
    exit 1
  fi
  if test -f $INSTALL_DIR/lib/libmongoc-1.0.a; then
    echo "libmongoc-1.0.a shouldn't have been installed"
    exit 1
  fi
  if test -f $INSTALL_DIR/lib/pkgconfig/libmongoc-static-1.0.pc; then
    echo "libmongoc-static-1.0.pc shouldn't have been installed"
    exit 1
  fi
fi

if [ "$OS" = "darwin" ]; then
  if test -f $INSTALL_DIR/bin/mongoc-stat; then
    echo "mongoc-stat shouldn't have been installed"
    exit 1
  fi
else
  if test ! -f $INSTALL_DIR/bin/mongoc-stat; then
    echo "mongoc-stat missing!"
    exit 1
  else
    echo "mongoc-stat check ok"
  fi
fi

if [ "$BUILD_SAMPLE_WITH_CMAKE" ]; then
  # Test our CMake package config file with CMake's find_package command.
  if [ "$BUILD_SAMPLE_WITH_CMAKE_DEPRECATED" ]; then
    EXAMPLE_DIR=$SRCROOT/src/libmongoc/examples/cmake-deprecated/find_package
  else
    EXAMPLE_DIR=$SRCROOT/src/libmongoc/examples/cmake/find_package
  fi

  if [ "$LINK_STATIC" ]; then
    EXAMPLE_DIR="${EXAMPLE_DIR}_static"
  fi

  cd $EXAMPLE_DIR
  $CMAKE -DCMAKE_PREFIX_PATH=$INSTALL_DIR/lib/cmake .
  $CMAKE --build .
else
  # Test our pkg-config file.
  export PKG_CONFIG_PATH=$INSTALL_DIR/lib/pkgconfig
  cd $SRCROOT/src/libmongoc/examples

  if [ "$LINK_STATIC" ]; then
    echo "pkg-config output:"
    echo $(pkg-config --libs --cflags libmongoc-static-1.0)
    sh compile-with-pkg-config-static.sh
  else
    echo "pkg-config output:"
    echo $(pkg-config --libs --cflags libmongoc-1.0)
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

echo "ldd hello_mongoc:"
$LDD hello_mongoc

./hello_mongoc
