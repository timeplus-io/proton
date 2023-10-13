if (NOT ENABLE_BENCHMARKS)
    message(STATUS "Not using google benchmark")
    return()
endif()

set (SRC_DIR "${proton_SOURCE_DIR}/contrib/benchmark")

set(SRCS
    ${SRC_DIR}/src/benchmark_api_internal.cc
    ${SRC_DIR}/src/benchmark.cc
    ${SRC_DIR}/src/benchmark_name.cc
    ${SRC_DIR}/src/benchmark_register.cc
    ${SRC_DIR}/src/benchmark_runner.cc
    ${SRC_DIR}/src/check.cc
    ${SRC_DIR}/src/colorprint.cc
    ${SRC_DIR}/src/commandlineflags.cc
    ${SRC_DIR}/src/complexity.cc
    ${SRC_DIR}/src/console_reporter.cc
    ${SRC_DIR}/src/counter.cc
    ${SRC_DIR}/src/csv_reporter.cc
    ${SRC_DIR}/src/json_reporter.cc
    ${SRC_DIR}/src/perf_counters.cc
    ${SRC_DIR}/src/reporter.cc
    ${SRC_DIR}/src/statistics.cc
    ${SRC_DIR}/src/string_util.cc
    ${SRC_DIR}/src/sysinfo.cc
    ${SRC_DIR}/src/timers.cc
)

add_library(google_benchmark "${SRCS}")
target_include_directories(google_benchmark SYSTEM PUBLIC "${SRC_DIR}/../include")

add_library(google_benchmark_main "${SRC_DIR}/benchmark_main.cc")
target_link_libraries(google_benchmark_main PUBLIC google_benchmark)

add_library(google_benchmark_all INTERFACE)
target_link_libraries(google_benchmark_all INTERFACE google_benchmark google_benchmark_main)

add_library(ch_contrib::gbenchmark_all ALIAS google_benchmark_all)

# FIXME(yokofly) support libpfm
# https://github.com/google/benchmark/blob/main/docs/perf_counters.md
