option (ENABLE_BENCHMARK "Enable google benchmark" ${ENABLE_BENCHMARK})

if (NOT ENABLE_BENCHMARK)
    message(STATUS "Not using google benchmark")
    return()
endif()

set (BENCHMARK_SOURCE_DIR "${proton_SOURCE_DIR}/contrib/benchmark")

set(BENCHMARK_SRC
    ${BENCHMARK_SOURCE_DIR}/src/arraysize.h
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_api_internal.cc
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_api_internal.h
    ${BENCHMARK_SOURCE_DIR}/src/benchmark.cc
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_name.cc
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_register.cc
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_register.h
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_runner.cc
    ${BENCHMARK_SOURCE_DIR}/src/benchmark_runner.h
    ${BENCHMARK_SOURCE_DIR}/src/check.cc
    ${BENCHMARK_SOURCE_DIR}/src/check.h
    ${BENCHMARK_SOURCE_DIR}/src/colorprint.cc
    ${BENCHMARK_SOURCE_DIR}/src/colorprint.h
    ${BENCHMARK_SOURCE_DIR}/src/commandlineflags.cc
    ${BENCHMARK_SOURCE_DIR}/src/commandlineflags.h
    ${BENCHMARK_SOURCE_DIR}/src/complexity.cc
    ${BENCHMARK_SOURCE_DIR}/src/complexity.h
    ${BENCHMARK_SOURCE_DIR}/src/console_reporter.cc
    ${BENCHMARK_SOURCE_DIR}/src/counter.cc
    ${BENCHMARK_SOURCE_DIR}/src/counter.h
    ${BENCHMARK_SOURCE_DIR}/src/csv_reporter.cc
    ${BENCHMARK_SOURCE_DIR}/src/cycleclock.h
    ${BENCHMARK_SOURCE_DIR}/src/internal_macros.h
    ${BENCHMARK_SOURCE_DIR}/src/json_reporter.cc
    ${BENCHMARK_SOURCE_DIR}/src/log.h
    ${BENCHMARK_SOURCE_DIR}/src/mutex.h
    ${BENCHMARK_SOURCE_DIR}/src/perf_counters.cc
    ${BENCHMARK_SOURCE_DIR}/src/perf_counters.h
    ${BENCHMARK_SOURCE_DIR}/src/re.h
    ${BENCHMARK_SOURCE_DIR}/src/reporter.cc
    ${BENCHMARK_SOURCE_DIR}/src/statistics.cc
    ${BENCHMARK_SOURCE_DIR}/src/statistics.h
    ${BENCHMARK_SOURCE_DIR}/src/string_util.cc
    ${BENCHMARK_SOURCE_DIR}/src/string_util.h
    ${BENCHMARK_SOURCE_DIR}/src/sysinfo.cc
    ${BENCHMARK_SOURCE_DIR}/src/thread_manager.h
    ${BENCHMARK_SOURCE_DIR}/src/thread_timer.h
    ${BENCHMARK_SOURCE_DIR}/src/timers.cc
    ${BENCHMARK_SOURCE_DIR}/src/timers.h
    ${BENCHMARK_SOURCE_DIR}/include/benchmark/benchmark.h
    ${BENCHMARK_SOURCE_DIR}/include/benchmark/export.h
)

# Build only the benchmark library
add_library(_benchmark ${BENCHMARK_SRC})

target_include_directories(_benchmark PRIVATE
  $<BUILD_INTERFACE:${BENCHMARK_SOURCE_DIR}/include>
)

# Link threads.
find_package(Threads REQUIRED)
target_link_libraries(_benchmark PRIVATE Threads::Threads)

# Expose the library target
add_library(ch_contrib::benchmark ALIAS _benchmark)

# FIXME(yokofly) support libpfm
# https://github.com/google/benchmark/blob/main/docs/perf_counters.md
