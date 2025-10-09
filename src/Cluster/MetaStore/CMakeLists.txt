set(metastore_headers
    ${CMAKE_CURRENT_SOURCE_DIR}/LocalMetaQueue.h
    ${CMAKE_CURRENT_SOURCE_DIR}/MetaDB.h
    ${CMAKE_CURRENT_SOURCE_DIR}/MetaStore.h
    ${CMAKE_CURRENT_SOURCE_DIR}/MultipleVersionMergeOperator.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ProposalRegistry.h
)

set(metastore_sources
    ${CMAKE_CURRENT_SOURCE_DIR}/LocalMetaQueue.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/MetaDB.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/MetaStore.cpp  # Core logic with all metadata operations
    ${CMAKE_CURRENT_SOURCE_DIR}/MetaStore_Bootstrap.cpp  # Constructor/destructor/init only
    ${CMAKE_CURRENT_SOURCE_DIR}/MetaStore_System.cpp  # System operations (log level, python)
    ${CMAKE_CURRENT_SOURCE_DIR}/MultipleVersionMergeOperator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ProposalRegistry.cpp
)

add_library(metastore ${metastore_headers} ${metastore_sources})

target_link_libraries(metastore PUBLIC  nlog)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()