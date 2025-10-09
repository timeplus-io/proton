include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(external_table .)

add_headers_and_sources(external_table S3)

add_library(external_table ${external_table_headers} ${external_table_sources})

target_link_libraries(external_table PUBLIC clickhouse_parsers ch_contrib::abseil_swiss_tables)

if (TARGET ch_contrib::aws_s3)
    target_link_libraries(external_table PUBLIC ch_contrib::aws_s3)
endif()

if (TARGET ch::mysqlxx)
    target_link_libraries(external_table PUBLIC ch::mysqlxx)
endif()

if (TARGET ch_contrib::libpqxx)
    target_link_libraries(external_table PUBLIC ch_contrib::libpqxx)
endif()

if (TARGET ch_contrib::mongocxx)
    target_link_libraries(external_table
        PUBLIC
            ch_contrib::libbson
            ch_contrib::libmongoc
            ch_contrib::bsoncxx
            ch_contrib::mongocxx
    )
endif ()

# if (ENABLE_TESTS)
#     add_subdirectory(tests)
# endif ()
