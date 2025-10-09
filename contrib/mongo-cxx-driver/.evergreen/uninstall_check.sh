#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

BUILD_DIR=$(pwd)/build
INSTALL_DIR=$BUILD_DIR/install

touch $INSTALL_DIR/lib/canary.txt

ls -l $INSTALL_DIR/share/mongo-cxx-driver

. .evergreen/find_cmake.sh

$CMAKE --build $BUILD_DIR --target uninstall

ls -lR $INSTALL_DIR

if test -f $INSTALL_DIR/lib/pkgconfig/libbsoncxx.pc; then
  echo "libbsoncxx.pc found!"
  exit 1
else
  echo "libbsoncxx.pc check ok"
fi
if test ! -f $INSTALL_DIR/lib/canary.txt; then
  echo "canary.txt not found!"
  exit 1
else
  echo "canary.txt check ok"
fi
if test ! -d $INSTALL_DIR/lib; then
  echo "$INSTALL_DIR/lib not found!"
  exit 1
else
  echo "$INSTALL_DIR/lib check ok"
fi
if test -f $INSTALL_DIR/lib/pkgconfig/libmongocxx.pc; then
  echo "libmongocxx.pc found!"
  exit 1
else
  echo "libmongocxx.pc check ok"
fi
if test -f $INSTALL_DIR/include/bsoncxx/v_noabi/bsoncxx/json.hpp; then
  echo "bsoncxx/json.hpp found!"
  exit 1
else
  echo "bsoncxx/json.hpp check ok"
fi
if test -f $INSTALL_DIR/include/bsoncxx/v_noabi/bsoncxx/types.hpp; then
  echo "bsoncxx/types.hpp found!"
  exit 1
else
  echo "bsoncxx/types.hpp check ok"
fi
if test -d $INSTALL_DIR/include/bsoncxx; then
  echo "$INSTALL_DIR/include/bsoncxx found!"
  exit 1
else
  echo "$INSTALL_DIR/include/bsoncxx check ok"
fi
if test -f $INSTALL_DIR/include/mongocxx/v_noabi/mongocxx/hint.hpp; then
  echo "mongocxx/hint.hpp found!"
  exit 1
else
  echo "mongocxx/hint.hpp check ok"
fi
if test -f $INSTALL_DIR/include/mongocxx/v_noabi/mongocxx/logger.hpp; then
  echo "mongocxx/logger.hpp found!"
  exit 1
else
  echo "mongocxx/logger.hpp check ok"
fi
if test -d $INSTALL_DIR/include/mongocxx; then
  echo "$INSTALL_DIR/include/mongocxx found!"
  exit 1
else
  echo "$INSTALL_DIR/include/mongocxx check ok"
fi
if test -f $INSTALL_DIR/share/mongo-cxx-driver/uninstall.sh; then
  echo "uninstall.sh found!"
  exit 1
else
  echo "uninstall.sh check ok"
fi
if test -d $INSTALL_DIR/share/mongo-cxx-driver; then
  echo "$INSTALL_DIR/share/mongo-cxx-driver found!"
  exit 1
else
  echo "$INSTALL_DIR/share/mongo-cxx-driver check ok"
fi
