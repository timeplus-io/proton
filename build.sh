#!/bin/bash

#Release, RelWithDebInfo, Debug
build_type="$1"
build_type="${build_type:=Release}"

is_not_darwin=1
if [[ "$(uname)" == "Darwin" ]]; then
    is_not_darwin=0
    echo "=== This is Darwin ==="
fi

sanitizer="$2"
if [ "$build_type" = "Debug" ] && [ $is_not_darwin = 1 ]; then
    if [ -z "$sanitizer" ]; then
        sanitizer="address"
    fi
fi

enable_parquet="ON"

enable_jemalloc="OFF"
if [ -z "$sanitizer" ]; then
    enable_jemalloc="ON"
fi

# cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -S . -B release-build

cmake .. \
    -DCMAKE_BUILD_TYPE="$build_type" \
    -DCMAKE_CXX_COMPILER=$(which clang++) \
    -DCMAKE_C_COMPILER=$(which clang) \
    -DENABLE_PROTON_ALL=OFF \
    -DENABLE_PROTON_SERVER=ON \
    -DENABLE_PROTON_CLIENT=ON \
    -DENABLE_PROTON_KLOG_BENCHMARK=ON \
    -DENABLE_PROTON_INSTALL=ON \
    -DENABLE_PROTON_NLOG=ON \
    -DENABLE_PROTON_PYTHON=ON \
    -DUSE_DEBUG_HELPERS=ON \
    -DENABLE_LIBRARIES=OFF \
    -DENABLE_BASE64=ON \
    -DENABLE_BENCHMARKS=${is_not_darwin} \
    -DENABLE_KAFKA=ON \
    -DENABLE_NURAFT=ON \
    -DENABLE_RAPIDJSON=ON \
    -DENABLE_YAML_CPP=ON \
    -DENABLE_SIMDJSON=ON \
    -DENABLE_ROCKSDB=ON \
    -DENABLE_JEMALLOC=${enable_jemalloc} \
    -DENABLE_SSL=ON \
    -DENABLE_BZIP2=ON \
    -DENABLE_BROTLI=ON \
    -DENABLE_PROTOBUF=ON \
    -DENABLE_LIBURING=${is_not_darwin} \
    -DENABLE_UTILS=ON \
    -DENABLE_THINLTO=OFF \
    -DENABLE_CLANG_TIDY=OFF \
    -DENABLE_TESTS=ON \
    -DENABLE_EXAMPLES=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DENABLE_CCACHE=ON \
    -DENABLE_CHECK_HEAVY_BUILDS=${is_not_darwin} \
    -DENABLE_GRPC=ON \
    -DENABLE_MATH_FUNCS=ON \
    -DENABLE_GEO_FUNCS=ON \
    -DENABLE_HIGH_ORDER_ARRAY_FUNCS=ON \
    -DENABLE_PARQUET=${enable_parquet} \
    -DENABLE_THRIFT=ON \
    -DENABLE_CYRUS_SASL=ON \
    -DENABLE_KRB5=ON \
    -DSANITIZE=${sanitizer} \
    -DENABLE_BITMAP_FUNCS=ON \
    -DENABLE_BINARY_REPR_FUNCS=ON \
    -DENABLE_IP_CODING_FUNCS=ON \
    -DENABLE_UUID_CODING_FUNCS=ON \
    -DENABLE_EXTERNAL_DICT_FUNCS=OFF \
    -DENABLE_FORMATTING_FUNCS=ON \
    -DENABLE_HASH_FUNCS=ON \
    -DENABLE_HIGH_ORDER_ARRAY_FUNCS=ON \
    -DENABLE_MISC_FUNCS=ON \
    -DENABLE_MATH_FUNCS=ON \
    -DENABLE_GEO_FUNCS=ON \
    -DENABLE_H3_GEO_FUNCS=ON \
    -DENABLE_S2_GEO_FUNCS=ON \
    -DENABLE_INTROSPECTION_FUNCS=ON \
    -DUSE_CONSISTENT_HASH_FUNCS=ON \
    -DENABLE_HAMMING_DISTANCE_FUNCS=ON \
    -DENABLE_SNOWFLAKE_FUNCS=ON \
    -DENABLE_ENCRYPT_DECRYPT_FUNCS=ON \
    -DENABLE_DEBUG_FUNCS=ON \
    -DENABLE_URL_FUNCS=ON \
    -DENABLE_ARG_MIN_MAX_FUNCS=OFF \
    -DENABLE_AVRO=ON \
    -DENABLE_PULSAR=${is_not_darwin} \
    -DENABLE_CURL=${is_not_darwin} \
    -DENABLE_PYTHON_UDF=ON \
    -DENABLE_AWS_S3=ON \
    -DENABLE_AWS_MSK_IAM=ON \
    -DENABLE_MYSQL=ON \
    -DENABLE_LIBPQXX=ON \
    -DENABLE_AZURE_BLOB_STORAGE=OFF \
    -DUSE_MONGODB=ON \
    -DSPLIT_DEBUG_SYMBOLS=ON
