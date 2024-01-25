add_subdirectory(divide)

include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp extractTimeZoneFromFunctionArguments.cpp FunctionsLogical.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h extractTimeZoneFromFunctionArguments.h FunctionsLogical.h)

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
            # currentDatabase.cpp
            currentProfiles.cpp
            currentRoles.cpp
            FunctionFQDN.cpp
            visibleWidth.cpp
            getSizeOfEnumType.cpp
            blockSerializedSize.cpp
            toColumnTypeName.cpp
            dumpColumnStructure.cpp
            blockSize.cpp
            blockNumber.cpp
            rowNumberInBlock.cpp
            # rowNumberInAllBlocks.cpp
            ignore.cpp
            indexHint.cpp
            # identity.cpp
            # bar.cpp
            hasColumnInTable.cpp
            # isFinite.cpp
            # isInfinite.cpp
            # isNaN.cpp
            # ifNotFinite.cpp
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
            # transform.cpp
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

add_library(clickhouse_functions_obj OBJECT ${clickhouse_functions_sources})

list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_obj>)

list (APPEND PUBLIC_LIBS
        ch_contrib::wyhash
        ch_contrib::cityhash
        ch_contrib::farmhash
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        ch_contrib::consistent_hashing
        dbms
        ch_contrib::metrohash
        ch_contrib::murmurhash
        ch_contrib::hashidsxx
        ch_contrib::morton_nd
)

list (APPEND PRIVATE_LIBS
        ch_contrib::zlib
        boost::filesystem
        divide_impl
        ch_contrib::xxHash
)

if (TARGET ch_rust::blake3)
    list (APPEND PUBLIC_LIBS
        ch_rust::blake3
    )
endif()

if (TARGET OpenSSL::Crypto)
    list (APPEND PUBLIC_LIBS OpenSSL::Crypto)
endif()

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_obj PRIVATE "-g0")
endif()

if (TARGET ch_contrib::icu)
    list (APPEND PRIVATE_LIBS ch_contrib::icu)
endif ()

if (TARGET ch_contrib::fastops)
    list (APPEND PRIVATE_LIBS ch_contrib::fastops)
endif ()

if (TARGET ch_contrib::llvm)
    list (APPEND PRIVATE_LIBS ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::base64)
    list (APPEND PRIVATE_LIBS ch_contrib::base64)
endif()

if (ENABLE_NLP)
    list (APPEND PRIVATE_LIBS ch_contrib::cld2)
endif()

if (TARGET ch_contrib::h3)
    list (APPEND PRIVATE_LIBS ch_contrib::h3)
endif()

if (TARGET ch_contrib::vectorscan)
    list (APPEND PRIVATE_LIBS ch_contrib::vectorscan)
endif()

if (TARGET ch_contrib::simdjson)
    list (APPEND PRIVATE_LIBS ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    list (APPEND PRIVATE_LIBS ch_contrib::rapidjson)
endif()

add_subdirectory(GatherUtils)
list (APPEND PRIVATE_LIBS clickhouse_functions_gatherutils)

if (ENABLE_URL_FUNCS)
    add_subdirectory(URL)
    list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_url>)
else()
    message(STATUS "URL functions are disabled")
endif()

add_subdirectory(array)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_array>)

if (TARGET ch_contrib::datasketches)
    add_subdirectory(UniqTheta)
    list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_uniqtheta>)
endif()

add_subdirectory(JSONPath)
list (APPEND PRIVATE_LIBS clickhouse_functions_jsonpath)

add_subdirectory(keyvaluepair)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_extractkeyvaluepairs>)

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()

# proton: starts.
add_subdirectory(Streaming)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_streaming>)
# proton: ends.

target_link_libraries(clickhouse_functions_obj PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})

# Used to forward the linking information to the final binaries such as clickhouse / unit_tests_dbms,
# since such information are lost after we convert to OBJECT target
add_library(clickhouse_functions INTERFACE)
target_link_libraries(clickhouse_functions INTERFACE ${OBJECT_LIBS} ${PUBLIC_LIBS} ${PRIVATE_LIBS})
