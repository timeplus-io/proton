set (CLICKHOUSE_LIBRARY_BRIDGE_SOURCES
    library-bridge.cpp
    LibraryInterface.cpp
    LibraryBridge.cpp
    Handlers.cpp
    HandlerFactory.cpp
    SharedLibraryHandler.cpp
    SharedLibraryHandlerFactory.cpp
)

if (OS_LINUX)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-export-dynamic")
endif ()

add_executable(proton-library-bridge ${CLICKHOUSE_LIBRARY_BRIDGE_SOURCES})

target_link_libraries(proton-library-bridge PRIVATE
    daemon
    dbms
    bridge
)

set_target_properties(proton-library-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

install(TARGETS proton-library-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
