#!/bin/bash

# get latest server
git clone -b ${SERVER_BRANCH} https://github.com/mariadb/server ../workdir-server

# copy C/C into libmariadb in server
ls -l ../workdir-server/libmariadb
cp -r . ../workdir-server/libmariadb

cd ../workdir-server
# don't pull in submodules. We want the latest C/C as libmariadb
git config cmake.update-submodules no
# build latest server with latest C/C as libmariadb
# skip to build some storage engines to speed up the build
cmake -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_SPIDER=NO -DPLUGIN_TOKUDB=NO
make -j9

cd mysql-test/
./mysql-test-run.pl --suite=main ${TEST_OPTION} --parallel=auto --skip-test=session_tracker_last_gtid
