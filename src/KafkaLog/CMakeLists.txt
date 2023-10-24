include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(klog .)

add_library(klog ${klog_headers} ${klog_sources})

target_link_libraries(klog PUBLIC nlog ch_contrib::rdkafka)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()
