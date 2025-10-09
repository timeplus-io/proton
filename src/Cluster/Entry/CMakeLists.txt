include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(cluster_entry .)

add_library(cluster_entry ${cluster_entry_headers} ${cluster_entry_sources})

target_link_libraries(cluster_entry
        PUBLIC
        # cluster_base
        cluster_common
        ch_contrib::abseil_swiss_tables)
