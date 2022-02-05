include(configure_config.cmake)
configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

add_subdirectory(divide)

include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

# Enable / disable functions
if (NOT ENABLE_BITMAP_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources FunctionsBitmap.cpp)
    list(REMOVE_ITEM clickhouse_functions_headers FunctionsBitmap.h)
    message(STATUS "Bitmap functions are disabled")
endif()

if (NOT ENABLE_BINARY_REPR_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources FunctionsBinaryRepr.cpp)
    message(STATUS "Binary repr functions are disabled")
endif()

if (NOT ENABLE_IP_CODING_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources FunctionsCodingIP.cpp)
    message(STATUS "IP functions are disabled")
endif()

if (NOT ENABLE_UUID_CODING_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources FunctionsCodingUUID.cpp)
    message(STATUS "UUID functions are disabled")
endif()

if (NOT ENABLE_EXTERNAL_DICT_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources FunctionsExternalDictionaries.cpp)
    list(REMOVE_ITEM clickhouse_functions_headers FunctionsExternalDictionaries.h)
    message(STATUS "External dictionary functions are disabled")
endif()

if (NOT ENABLE_EXTERNAL_MODELS_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources modelEvaluate.cpp)
    message(STATUS "External models functions are disabled")
endif()

if (NOT ENABLE_FORMATTING_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            registerFunctionsFormatting.cpp
            FunctionsBitToArray.cpp
            formatReadableSize.cpp
            formatReadableQuantity.cpp
            formatReadableTimeDelta.cpp
            )
    message(STATUS "Formatting functions are disabled")
endif()

if (NOT ENABLE_HASH_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources FunctionsHashing.cpp)
    list(REMOVE_ITEM clickhouse_functions_headers FunctionsHashing.h)
    message(STATUS "Hash functions are disabled")
endif()

if (NOT ENABLE_HIGH_ORDER_ARRAY_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources registerFunctionsHigherOrder.cpp)
    message(STATUS "High ordered array functions are disabled")
endif()

if (NOT ENABLE_MISC_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            currentDatabase.cpp
            currentUser.cpp
            currentProfiles.cpp
            currentRoles.cpp
            serverConstants.cpp
            FunctionFQDN.cpp
            visibleWidth.cpp
            getSizeOfEnumType.cpp
            blockSerializedSize.cpp
            toColumnTypeName.cpp
            dumpColumnStructure.cpp
            blockSize.cpp
            blockNumber.cpp
            rowNumberInBlock.cpp
            rowNumberInAllBlocks.cpp
            ignore.cpp
            indexHint.cpp
            identity.cpp
            bar.cpp
            hasColumnInTable.cpp
            isFinite.cpp
            isInfinite.cpp
            isNaN.cpp
            ifNotFinite.cpp
            throwIf.cpp
            runningAccumulate.cpp
            runningDifference.cpp
            runningDifferenceStartingWithFirstValue.cpp
            runningConcurrency.cpp
            toLowCardinality.cpp
            lowCardinalityIndices.cpp
            lowCardinalityKeys.cpp
            filesystem.cpp
            evalMLMethod.cpp
            transform.cpp
            getMacro.cpp
            getScalar.cpp
            getSetting.cpp
            isConstant.cpp
            isDecimalOverflow.cpp
            countDigits.cpp
            globalVariable.cpp
            hasThreadFuzzer.cpp
            byteSize.cpp
            )
    message(STATUS "Misc functions are disabled")
endif()

if (NOT ENABLE_MATH_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            mathConstants.cpp
            exp.cpp
            log.cpp
            exp2.cpp
            log2.cpp
            log1p.cpp
            exp10.cpp
            log10.cpp
            sqrt.cpp
            cbrt.cpp
            erf.cpp
            erfc.cpp
            lgamma.cpp
            tgamma.cpp
            sin.cpp
            cos.cpp
            tan.cpp
            asin.cpp
            acos.cpp
            atan.cpp
            atan2.cpp
            sigmoid.cpp
            hypot.cpp
            sinh.cpp
            cosh.cpp
            tanh.cpp
            asinh.cpp
            acosh.cpp
            atanh.cpp
            degrees.cpp
            radians.cpp
            )
endif()

if (NOT ENABLE_GEO_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            greatCircleDistance.cpp
            pointInEllipses.cpp
            pointInPolygon.cpp
            polygonsIntersection.cpp
            polygonsUnion.cpp
            polygonArea.cpp
            polygonConvexHull.cpp
            polygonsSymDifference.cpp
            polygonsEquals.cpp
            polygonsDistance.cpp
            polygonsWithin.cpp
            polygonPerimeter.cpp
            geohashEncode.cpp
            geohashDecode.cpp
            geohashesInBox.cpp
            wkt.cpp
            svg.cpp
            )
    message(STATUS "Geo functions are disabled")
endif()

if (NOT ENABLE_H3_GEO_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            geoToH3.cpp
            h3toGeo.cpp
            h3ToGeoBoundary.cpp
            h3EdgeAngle.cpp
            h3EdgeLengthM.cpp
            h3EdgeLengthKm.cpp
            h3ExactEdgeLengthM.cpp
            h3ExactEdgeLengthKm.cpp
            h3ExactEdgeLengthRads.cpp
            h3GetResolution.cpp
            h3IsValid.cpp
            h3kRing.cpp
            h3GetBaseCell.cpp
            h3ToParent.cpp
            h3ToChildren.cpp
            h3ToCenterChild.cpp
            h3IndexesAreNeighbors.cpp
            stringToH3.cpp
            h3ToString.cpp
            h3HexAreaM2.cpp
            h3IsResClassIII.cpp
            h3IsPentagon.cpp
            h3GetFaces.cpp
            h3HexAreaKm2.cpp
            h3CellAreaM2.cpp
            h3CellAreaRads2.cpp
            h3NumHexagons.cpp
            )
    message(STATUS "H3 geo functions are disabled")
endif()

if (NOT ENABLE_S2_GEO_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            geoToS2.cpp
            s2ToGeo.cpp
            s2GetNeighbors.cpp
            s2CellsIntersect.cpp
            s2CapContains.cpp
            s2RectAdd.cpp
            s2RectContains.cpp
            s2RectUnion.cpp
            s2RectIntersection.cpp
            )
    message(STATUS "S2 geo functions are disabled")
endif()

if (NOT ENABLE_INTROSPECTION_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            addressToSymbol.cpp
            addressToLine.cpp
            demange.cpp
            trap.cpp
            registerFunctionsIntrospection.cpp
            )
    message(STATUS "Introspection functions are disabled")
endif()

if (NOT USE_CONSISTENT_HASH_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            registerFunctionsConsistentHashing.cpp
            )
    message(STATUS "Consistent hash functions are disabled")
endif()

if (NOT ENABLE_HAMMING_DISTANCE_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            bitHammingDistance.cpp
            tupleHammingDistance.cpp
            )
    message(STATUS "Hamming distance functions are disabled")
endif()

if (NOT ENABLE_SNOWFLAKE_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            snowflake.cpp
            registerFunctionsSnowflake.cpp
            )
    message(STATUS "Snowflake functions are disabled")
endif()

if (NOT ENABLE_ENCRYPT_DECRYPT_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            encrypt.cpp
            decrypt.cpp
            aes_encrypt_mysql.cpp
            aes_decrypt_mysql.cpp
            )
    message(STATUS "Encrypt/Decrypt functions are disabled")
endif()

if (NOT ENABLE_DEBUG_FUNCS)
    list(REMOVE_ITEM clickhouse_functions_sources
            tid.cpp
            logTrace.cpp
            )
    message(STATUS "Debug functions are disabled")
endif()

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions
    PUBLIC
        ch_contrib::cityhash
        ch_contrib::farmhash
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        ch_contrib::consistent_hashing
        dbms
        ch_contrib::metrohash
        ch_contrib::murmurhash

    PRIVATE
        ch_contrib::zlib
        boost::filesystem
        divide_impl
)

if (TARGET OpenSSL::Crypto)
    target_link_libraries(clickhouse_functions PUBLIC OpenSSL::Crypto)
endif()

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    set (STRIP_DSF_DEFAULT ON)
else()
    set (STRIP_DSF_DEFAULT OFF)
endif()


#  Provides faster linking and lower binary size.
#  Tradeoff is the inability to debug some source files with e.g. gdb
#  (empty stack frames and no local variables)."
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS "Do not generate debugger info for ClickHouse functions" ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    message(WARNING "Not generating debugger info for ClickHouse functions")
    target_compile_options(clickhouse_functions PRIVATE "-g0")
else()
    message(STATUS "Generating debugger info for ClickHouse functions")
endif()

if (TARGET ch_contrib::icu)
    target_link_libraries (clickhouse_functions PRIVATE ch_contrib::icu)
endif ()

if (TARGET ch_contrib::fastops)
    target_link_libraries (clickhouse_functions PRIVATE ch_contrib::fastops)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (TARGET ch_contrib::llvm)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::base64)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::base64)
endif()

target_link_libraries(clickhouse_functions PRIVATE ch_contrib::lz4)

if (ENABLE_NLP)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::cld2)
endif()

if (TARGET ch_contrib::h3)
    target_link_libraries (clickhouse_functions PRIVATE ch_contrib::h3)
endif()

if (TARGET ch_contrib::hyperscan)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::hyperscan)
endif()

if (TARGET ch_contrib::simdjson)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::rapidjson)
endif()

# ClickHouse developers may use platform-dependent code under some macro (e.g. `#ifdef ENABLE_MULTITARGET`).
# If turned ON, this option defines such macro.
# See `src/Functions/TargetSpecific.h`
option(ENABLE_MULTITARGET_CODE "Enable platform-dependent code" ON)

if (ENABLE_MULTITARGET_CODE)
    add_definitions(-DENABLE_MULTITARGET_CODE=1)
else()
    add_definitions(-DENABLE_MULTITARGET_CODE=0)
endif()

add_subdirectory(GatherUtils)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_gatherutils)

if (ENABLE_URL_FUNCS)
    add_subdirectory(URL)
    target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_url)
else()
    message(STATUS "URL functions are disabled")
endif()


add_subdirectory(array)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_array)

add_subdirectory(JSONPath)

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()
