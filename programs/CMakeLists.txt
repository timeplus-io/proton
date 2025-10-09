if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

include(${proton_SOURCE_DIR}/cmake/split_debug_symbols.cmake)

# The `proton` binary is a multi purpose tool that contains multiple execution modes (client, server, etc.),
# each of them may be built and linked as a separate library.
# If you do not know what modes you need, turn this option OFF and enable SERVER and CLIENT only.
option (ENABLE_PROTON_ALL "Enable all proton modes by default" ON)

option (ENABLE_PROTON_SERVER "Server mode (main mode)" ${ENABLE_PROTON_ALL})
option (ENABLE_PROTON_CLIENT "Client mode (interactive tui/shell that connects to the server)"
    ${ENABLE_PROTON_ALL})

# https://clickhouse.tech/docs/en/operations/utilities/clickhouse-local/
option (ENABLE_PROTON_LOCAL "Local files fast processing mode" ${ENABLE_PROTON_ALL})

# https://clickhouse.tech/docs/en/operations/utilities/clickhouse-benchmark/
option (ENABLE_PROTON_BENCHMARK "Queries benchmarking mode" ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_FORMAT "Queries pretty-printer and formatter with syntax highlighting"
    ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_KLOG_BENCHMARK "Kafka write ahead log benchmark" ${ENABLE_PROTON_ALL})

# https://presentations.clickhouse.com/matemarketing_2020/
option (ENABLE_PROTON_GIT_IMPORT "A tool to analyze Git repositories" ${ENABLE_PROTON_ALL})

# https://clickhouse.com/docs/en/operations/utilities/clickhouse-compressor/
option (ENABLE_PROTON_COMPRESSOR "Data compressor and decompressor" ${ENABLE_proton_ALL})

# proton: starts.
option (ENABLE_PROTON_NLOG "A tool to benchmark nlog performance" ${ENABLE_PROTON_ALL})
option (ENABLE_PROTON_META "A tool to examine meta data" ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_PYTHON "Python interpreter" ${ENABLE_PROTON_ALL})
if (NOT ENABLE_PYTHON_UDF)
    set(ENABLE_PROTON_PYTHON OFF)
endif()
# proton: ends.

option(ENABLE_PROTON_INSTALL "Install proton without .deb/.rpm/.tgz packages (having the binary only)"
        ${ENABLE_PROTON_ALL})

message(STATUS "proton modes:")

if (NOT ENABLE_PROTON_SERVER)
    message(WARNING "proton server mode is not going to be built.")
else()
    message(STATUS "Server mode: ON")
endif()

if (NOT ENABLE_PROTON_CLIENT)
    message(WARNING "proton client mode is not going to be built. You won't be able to connect to the server and run tests")
else()
    message(STATUS "Client mode: ON")
endif()

if (ENABLE_PROTON_LOCAL)
    message(STATUS "Local mode: ON")
else()
    message(STATUS "Local mode: OFF")
endif()

if (ENABLE_PROTON_BENCHMARK)
    message(STATUS "Benchmark mode: ON")
else()
    message(STATUS "Benchmark mode: OFF")
endif()

if (ENABLE_PROTON_FORMAT)
    message(STATUS "Format mode: ON")
else()
    message(STATUS "Format mode: OFF")
endif()

if (ENABLE_PROTON_INSTALL)
    message(STATUS "proton install: ON")
else()
    message(STATUS "proton install: OFF")
endif()

if (ENABLE_PROTON_GIT_IMPORT)
    message(STATUS "proton git-import: ON")
else()
    message(STATUS "proton git-import: OFF")
endif()

if (ENABLE_PROTON_COMPRESSOR)
    message(STATUS "Compressor mode: ON")
else()
    message(STATUS "Compressor mode: OFF")
endif()

# proton: starts.
if (ENABLE_PROTON_NLOG)
    message(STATUS "proton nlog mode: ON")
else()
    message(STATUS "proton nlog mode: OFF")
endif()

if (ENABLE_PROTON_META)
    message(STATUS "proton meta mode: ON")
else()
    message(STATUS "proton meta mode: OFF")
endif()

if (ENABLE_PROTON_PYTHON)
    message(STATUS "proton python mode: ON")
else()
    message(STATUS "proton python mode: OFF")
endif()
# proton: ends.

if (ENABLE_PROTON_KLOG_BENCHMARK)
    message(STATUS "proton klog-benchmark: ON")
else()
    message(STATUS "proton klog-benchmark: OFF")
endif()

configure_file (config_tools.h.in ${CONFIG_INCLUDE_PATH}/config_tools.h)

macro(proton_target_link_split_lib target name)
    target_link_libraries(${target} PRIVATE clickhouse-${name}-lib)
endmacro()

macro(proton_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(CLICKHOUSE_${name_uc}_SOURCES ${CLICKHOUSE_${name_uc}_SOURCES} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_LINK ${CLICKHOUSE_${name_uc}_LINK} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_INCLUDE ${CLICKHOUSE_${name_uc}_INCLUDE} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_DEFINITIONS ${CLICKHOUSE_${name_uc}_DEFINITIONS} PARENT_SCOPE)

    add_library(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_SOURCES})

    set(_link ${CLICKHOUSE_${name_uc}_LINK}) # can't use ${} in if()
    if(_link)
        target_link_libraries(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_LINK})
    endif()

    set(_include ${CLICKHOUSE_${name_uc}_INCLUDE}) # can't use ${} in if()
    if (_include)
        target_include_directories(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_INCLUDE})
    endif()

    set(_definitions ${CLICKHOUSE_${name_uc}_DEFINITIONS}) # can't use ${} in if()
    if (_definitions)
        target_compile_definitions(clickhouse-${name}-lib PRIVATE ${CLICKHOUSE_${name_uc}_DEFINITIONS})
    endif()
endmacro()

macro(proton_program_add name)
    proton_program_add_library(${name})
endmacro()

add_subdirectory (server)
add_subdirectory (client)
add_subdirectory (local)
add_subdirectory (benchmark)
add_subdirectory (format)
add_subdirectory (install)
add_subdirectory (bash-completion)
# proton: starts
add_subdirectory (klog)
# proton: ends
add_subdirectory (git-import)
add_subdirectory (compressor)

# proton: starts.
if (ENABLE_PROTON_NLOG)
    add_subdirectory (native_log)
endif()

if (ENABLE_PROTON_META)
    add_subdirectory (meta)
endif()

if (ENABLE_PROTON_PYTHON)
    add_subdirectory (python)
endif()
# proton: ends.


proton_add_executable (proton main.cpp)

# A library that prevent usage of several functions from libc.
if (ARCH_AMD64 AND OS_LINUX AND NOT OS_ANDROID AND NOT ENABLE_PYTHON_UDF)
    set (HARMFUL_LIB harmful)
endif ()

target_link_libraries (proton PRIVATE clickhouse_common_io string_utils ${HARMFUL_LIB})
target_include_directories (proton PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

if (NOT APPLE)
    # Disable symbols.map link options on MacOS since it doesn't support this
    set_target_properties(proton PROPERTIES LINK_FLAGS  "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif ()

if (ENABLE_PROTON_SERVER)
    proton_target_link_split_lib(proton server)
endif ()
if (ENABLE_PROTON_CLIENT)
    proton_target_link_split_lib(proton client)
endif ()
if (ENABLE_PROTON_LOCAL)
    proton_target_link_split_lib(proton local)
endif ()
if (ENABLE_PROTON_BENCHMARK)
    proton_target_link_split_lib(proton benchmark)
endif ()
if (ENABLE_PROTON_FORMAT)
    proton_target_link_split_lib(proton format)
endif ()
if (ENABLE_PROTON_GIT_IMPORT)
    proton_target_link_split_lib(proton git-import)
endif ()
if (ENABLE_PROTON_COMPRESSOR)
    proton_target_link_split_lib(proton compressor)
endif ()
# proton: starts.
if (ENABLE_PROTON_NLOG)
    proton_target_link_split_lib(proton nlog)
endif()

if (ENABLE_PROTON_META)
    proton_target_link_split_lib(proton meta)
endif()

if (ENABLE_PROTON_PYTHON)
    proton_target_link_split_lib(proton python)
endif()
# proton: ends.
if (ENABLE_PROTON_INSTALL)
    proton_target_link_split_lib(proton install)
endif ()
if (ENABLE_PROTON_KLOG_BENCHMARK)
    # proton: starts
    proton_target_link_split_lib(proton klog-benchmark)
    # proton: ends
endif ()

set (PROTON_BUNDLE)
if (ENABLE_PROTON_SERVER)
    add_custom_target (proton-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-server DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-server" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-server)
endif ()
if (ENABLE_PROTON_CLIENT)
    add_custom_target (proton-client ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-client DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-client" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-client)
endif ()
if (ENABLE_PROTON_LOCAL)
    add_custom_target (proton-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-local DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-local" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-local)
endif ()
if (ENABLE_PROTON_BENCHMARK)
    add_custom_target (proton-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-benchmark DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-benchmark" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-benchmark)
endif ()
if (ENABLE_PROTON_FORMAT)
    add_custom_target (proton-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-format DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-format" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-format)
endif ()
if (ENABLE_PROTON_GIT_IMPORT)
    add_custom_target (proton-git-import ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-git-import DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-git-import" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-git-import)
endif ()
if (ENABLE_PROTON_COMPRESSOR)
    add_custom_target (proton-compressor ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-compressor DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-compressor" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-compressor)
endif ()
# proton: starts.
if (ENABLE_PROTON_NLOG)
    add_custom_target (proton-nlog ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-nlog DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-nlog" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-nlog)
endif ()

if (ENABLE_PROTON_META)
    add_custom_target (proton-meta ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-meta DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-meta" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-meta)
endif ()

if (ENABLE_PROTON_PYTHON)
    add_custom_target (proton-python ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-python DEPENDS proton)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-python" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-python)
endif()
# proton: ends.
if (ENABLE_PROTON_KLOG_BENCHMARK)
    # proton: starts
    add_custom_target (proton-klog-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-klog-benchmark DEPENDS proton)
    install (FILES ${CMAKE_CURRENT_BINARY_DIR}/proton-klog-benchmark DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    list(APPEND PROTON_BUNDLE proton-klog-benchmark)
    # proton: ends
endif ()

add_custom_target (proton-bundle ALL DEPENDS ${PROTON_BUNDLE})

if (USE_BINARY_HASH)
    add_custom_command(TARGET proton POST_BUILD COMMAND ./proton hash-binary > hash && ${OBJCOPY_PATH} --add-section .proton.hash=hash proton COMMENT "Adding section '.proton.hash' to proton binary" VERBATIM)
endif()

if (SPLIT_DEBUG_SYMBOLS)
    proton_split_debug_symbols(TARGET proton DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH proton)
else()
    proton_make_empty_debug_info_for_nfpm(TARGET proton DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SPLITTED_DEBUG_SYMBOLS_DIR})
    install (TARGETS proton RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
endif()

if (ENABLE_TESTS)
    set (CLICKHOUSE_UNIT_TESTS_TARGETS unit_tests_dbms)
    add_custom_target (proton-tests ALL DEPENDS ${CLICKHOUSE_UNIT_TESTS_TARGETS})
    add_dependencies(proton-bundle proton-tests)
endif()

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()

if (TARGET ch_contrib::protobuf)
    get_property(google_proto_files TARGET ch_contrib::protobuf PROPERTY google_proto_files)
    message(WARNING "PROTO FILES ${google_proto_files}")
    foreach (proto_file IN LISTS google_proto_files)
        install(FILES ${proto_file} DESTINATION ${CMAKE_INSTALL_DATADIR}/proton/protos/google/protobuf)
    endforeach()
endif ()
