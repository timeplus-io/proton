#!/bin/bash
set -o errexit
set -o pipefail

BUILD_TYPE=${BUILD_TYPE:-Release}
CXX_STANDARD=${CXX_STANDARD:-11}
CMAKE=${cmake_binary:-cmake}

rm -rf build/*
cd build
if [ -z "$MSVC" ]; then
    "$CMAKE" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DCMAKE_CXX_STANDARD="${CXX_STANDARD}" ..
    "$CMAKE" --build . --target run
else
    if [ "$CXX_STANDARD" = "17" ]; then
        "$CMAKE" -G "Visual Studio 15 2017" -A "x64" -DCMAKE_CXX_STANDARD="${CXX_STANDARD}" ..
    else
        # Boost is needed for pre-17 Windows polyfill.
        "$CMAKE" -G "Visual Studio 14 2015" -A "x64" -DCMAKE_CXX_STANDARD="${CXX_STANDARD}" -DBOOST_ROOT=c:/local/boost_1_60_0 ..
    fi
    "$CMAKE" --build . --target run --config "${BUILD_TYPE}" -- /verbosity:minimal
fi
