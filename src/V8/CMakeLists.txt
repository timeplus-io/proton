include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(V8 .)
add_headers_and_sources(V8 Modules)

add_library(V8 ${V8_headers} ${V8_sources})

target_link_libraries(
        V8
        PUBLIC
        clickhouse_common_io
        ch_contrib::v8_base_without_compiler
        ch_contrib::v8_compiler
        ch_contrib::v8_snapshot
        ch_contrib::v8_inspector
        ch_contrib::v8_libplatform
        ch_contrib::v8_libbase
        ch_contrib::v8_libsampler
        ch_contrib::v8_initializers
        ch_contrib::v8_torque_generated
        Threads::Threads
        ${CMAKE_DL_LIBS}
)

target_compile_options(V8 PUBLIC -Wno-newline-eof -Wno-non-virtual-dtor -Wno-undef -Wno-unused-parameter -Wno-documentation -Wno-undefined-reinterpret-cast -Wno-shadow-field-in-constructor)

#if (ENABLE_TESTS)
#    add_subdirectory(tests)
#endif ()
#
if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

