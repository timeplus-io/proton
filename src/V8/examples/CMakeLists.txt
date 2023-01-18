proton_add_executable (v8_perf v8_perf.cpp)

target_link_libraries(
        v8_perf
        PRIVATE
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
        loggers
)

target_compile_options(v8_perf PUBLIC -Wno-newline-eof -Wno-non-virtual-dtor -Wno-undef -Wno-unused-parameter -Wno-documentation -Wno-undefined-reinterpret-cast -Wno-shadow-field-in-constructor)

target_include_directories(v8_perf
        PRIVATE
        "${proton_SOURCE_DIR}/src")