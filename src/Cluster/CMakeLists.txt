include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_subdirectory(Base)

add_subdirectory(Rocks)

add_subdirectory(Common)

add_subdirectory(Protocol)

add_subdirectory(Requests)

add_subdirectory(Entry)

add_subdirectory(LocalLog)

add_subdirectory(SchemaRecord)

add_subdirectory(KafkaLog)

add_subdirectory(MetaStore)

add_subdirectory(NativeLog)

add_headers_and_sources(cluster .)

add_library(cluster ${cluster_headers} ${cluster_sources})

target_link_libraries(cluster
        PUBLIC
        # cluster_base
        cluster_rocks
        cluster_common
        cluster_entry 
        cluster_protocol
        cluster_requests
        nlog
        metastore
        datastore
        klog
)

# if (ENABLE_EXAMPLES)
#     add_subdirectory(examples)
# endif ()
