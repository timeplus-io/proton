set(datastore_headers
    ${CMAKE_CURRENT_SOURCE_DIR}/NativeLog.h
)

set(datastore_sources
    ${CMAKE_CURRENT_SOURCE_DIR}/NativeLog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/NativeLog_Init.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/NativeLog_Action.cpp
)

add_library(datastore ${datastore_headers} ${datastore_sources})

target_link_libraries(datastore PUBLIC nlog cluster_rocks)
