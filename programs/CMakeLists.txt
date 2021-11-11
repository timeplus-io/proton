if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

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

option (ENABLE_PROTON_EXTRACT_FROM_CONFIG "Configs processor (extract values etc.)" ${ENABLE_PROTON_ALL})

# https://clickhouse.tech/docs/en/operations/utilities/clickhouse-compressor/
option (ENABLE_PROTON_COMPRESSOR "Data compressor and decompressor" ${ENABLE_PROTON_ALL})

# https://clickhouse.tech/docs/en/operations/utilities/clickhouse-copier/
option (ENABLE_PROTON_COPIER "Inter-cluster data copying mode" ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_FORMAT "Queries pretty-printer and formatter with syntax highlighting"
    ${ENABLE_PROTON_ALL})

# https://clickhouse.tech/docs/en/operations/utilities/clickhouse-obfuscator/
option (ENABLE_PROTON_OBFUSCATOR "Table data obfuscator (convert real data to benchmark-ready one)"
    ${ENABLE_PROTON_ALL})

# https://clickhouse.com/docs/en/operations/utilities/odbc-bridge/
# TODO Also needs NANODBC.
if (ENABLE_ODBC)
    option (ENABLE_PROTON_ODBC_BRIDGE "HTTP-server working like a proxy to ODBC driver"
        ${ENABLE_PROTON_ALL})
else ()
    option (ENABLE_PROTON_ODBC_BRIDGE "HTTP-server working like a proxy to ODBC driver" OFF)
endif ()

option (ENABLE_PROTON_LIBRARY_BRIDGE "HTTP-server working like a proxy to Library dictionary source"
    ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_DWAL_BENCHMARK "Distributed write ahead log benchmark" ${ENABLE_PROTON_ALL})

# https://presentations.clickhouse.tech/matemarketing_2020/
option (ENABLE_PROTON_GIT_IMPORT "A tool to analyze Git repositories" ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_STATIC_FILES_DISK_UPLOADER "A tool to export table data files to be later put to a static files web server" ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_KEEPER "proton alternative to ZooKeeper" ${ENABLE_PROTON_ALL})

option (ENABLE_PROTON_KEEPER_CONVERTER "Util allows to convert ZooKeeper logs and snapshots into proton-keeper snapshot" ${ENABLE_PROTON_ALL})

if (NOT ENABLE_NURAFT)
    # RECONFIGURE_MESSAGE_LEVEL should not be used here,
    # since ENABLE_NURAFT is set to OFF for FreeBSD and Darwin.
    message (STATUS "proton-keeper and proton-keeper-converter will not be built (lack of NuRaft)")
    set(ENABLE_PROTON_KEEPER OFF)
    set(ENABLE_PROTON_KEEPER_CONVERTER OFF)
endif()

# proton: starts.
option (ENABLE_PROTON_METASTORE "Proton alternative to MetaStore" ${ENABLE_PROTON_ALL})
if (NOT USE_NURAFT)
    # RECONFIGURE_MESSAGE_LEVEL should not be used here,
    # since USE_NURAFT is set to OFF for FreeBSD and Darwin.
    message (STATUS "proton-metastore will not be built (lack of NuRaft)")
    set(ENABLE_PROTON_METASTORE OFF)
endif()
# proton: ends.

if (PROTON_SPLIT_BINARY)
    option(ENABLE_PROTON_INSTALL "Install proton without .deb/.rpm/.tgz packages (having the binary only)" OFF)
else ()
    option(ENABLE_PROTON_INSTALL "Install proton without .deb/.rpm/.tgz packages (having the binary only)"
        ${ENABLE_PROTON_ALL})
endif ()

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

if (ENABLE_PROTON_EXTRACT_FROM_CONFIG)
    message(STATUS "Extract from config mode: ON")
else()
    message(STATUS "Extract from config mode: OFF")
endif()

if (ENABLE_PROTON_COMPRESSOR)
    message(STATUS "Compressor mode: ON")
else()
    message(STATUS "Compressor mode: OFF")
endif()

if (ENABLE_PROTON_COPIER)
    message(STATUS "Copier mode: ON")
else()
    message(STATUS "Copier mode: OFF")
endif()

if (ENABLE_PROTON_FORMAT)
    message(STATUS "Format mode: ON")
else()
    message(STATUS "Format mode: OFF")
endif()

if (ENABLE_PROTON_OBFUSCATOR)
    message(STATUS "Obfuscator mode: ON")
else()
    message(STATUS "Obfuscator mode: OFF")
endif()

if (ENABLE_PROTON_ODBC_BRIDGE)
    message(STATUS "ODBC bridge mode: ON")
else()
    message(STATUS "ODBC bridge mode: OFF")
endif()

if (ENABLE_PROTON_LIBRARY_BRIDGE)
    message(STATUS "Library bridge mode: ON")
else()
    message(STATUS "Library bridge mode: OFF")
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

if (ENABLE_PROTON_KEEPER)
    message(STATUS "proton keeper mode: ON")
else()
    message(STATUS "proton keeper mode: OFF")
endif()

# proton: starts.
if (ENABLE_PROTON_METASTORE)
    message(STATUS "proton metastore mode: ON")
else()
    message(STATUS "proton metastore mode: OFF")
endif()
# proton: ends.

if (ENABLE_PROTON_KEEPER_CONVERTER)
    message(STATUS "proton keeper-converter mode: ON")
else()
    message(STATUS "proton keeper-converter mode: OFF")
endif()

if (ENABLE_PROTON_DWAL_BENCHMARK)
    message(STATUS "proton dwal-benchmark: ON")
else()
    message(STATUS "proton dwal-benchmark: OFF")
endif()

if(NOT (USE_STATIC_LIBRARIES OR SPLIT_SHARED_LIBRARIES))
    set(CLICKHOUSE_ONE_SHARED ON)
endif()

configure_file (config_tools.h.in ${ConfigIncludePath}/config_tools.h)

macro(clickhouse_target_link_split_lib target name)
    if(NOT CLICKHOUSE_ONE_SHARED)
        target_link_libraries(${target} PRIVATE clickhouse-${name}-lib)
    else()
        target_link_libraries(${target} PRIVATE clickhouse-lib)
    endif()
endmacro()

macro(clickhouse_program_link_split_binary name)
    clickhouse_target_link_split_lib(proton-${name} ${name})
endmacro()

macro(clickhouse_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(CLICKHOUSE_${name_uc}_SOURCES ${CLICKHOUSE_${name_uc}_SOURCES} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_LINK ${CLICKHOUSE_${name_uc}_LINK} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_INCLUDE ${CLICKHOUSE_${name_uc}_INCLUDE} PARENT_SCOPE)

    if(NOT CLICKHOUSE_ONE_SHARED)
        add_library(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_SOURCES})

        set(_link ${CLICKHOUSE_${name_uc}_LINK}) # can't use ${} in if()
        if(_link)
            target_link_libraries(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_LINK})
        endif()

        set(_include ${CLICKHOUSE_${name_uc}_INCLUDE}) # can't use ${} in if()
        if (_include)
            target_include_directories(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_INCLUDE})
        endif()
    endif()
endmacro()

macro(clickhouse_program_add_executable name)
    if(PROTON_SPLIT_BINARY)
        add_executable(proton-${name} clickhouse-${name}.cpp)
        clickhouse_program_link_split_binary(${name})
        install(TARGETS proton-${name} ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    endif()
endmacro()

macro(clickhouse_program_add name)
    clickhouse_program_add_library(${name})
    clickhouse_program_add_executable(${name})
endmacro()

add_subdirectory (server)
add_subdirectory (client)
add_subdirectory (local)
add_subdirectory (benchmark)
add_subdirectory (extract-from-config)
add_subdirectory (compressor)
add_subdirectory (copier)
add_subdirectory (format)
add_subdirectory (obfuscator)
add_subdirectory (install)
add_subdirectory (git-import)
add_subdirectory (bash-completion)
add_subdirectory (static-files-disk-uploader)
add_subdirectory (distributed-wal)

if (ENABLE_PROTON_KEEPER)
    add_subdirectory (keeper)
endif()

if (ENABLE_PROTON_KEEPER_CONVERTER)
    add_subdirectory (keeper-converter)
endif()

# proton: starts.
if (ENABLE_PROTON_METASTORE)
    add_subdirectory (metastore)
endif()
# proton: ends.

if (ENABLE_PROTON_ODBC_BRIDGE)
    add_subdirectory (odbc-bridge)
endif ()

if (ENABLE_PROTON_LIBRARY_BRIDGE)
    add_subdirectory (library-bridge)
endif ()

if (CLICKHOUSE_ONE_SHARED)
  add_library(clickhouse-lib SHARED
    ${CLICKHOUSE_SERVER_SOURCES}
    ${CLICKHOUSE_CLIENT_SOURCES}
    ${CLICKHOUSE_LOCAL_SOURCES}
    ${CLICKHOUSE_BENCHMARK_SOURCES}
    ${CLICKHOUSE_COPIER_SOURCES}
    ${CLICKHOUSE_EXTRACT_FROM_CONFIG_SOURCES}
    ${CLICKHOUSE_COMPRESSOR_SOURCES}
    ${CLICKHOUSE_FORMAT_SOURCES}
    ${CLICKHOUSE_OBFUSCATOR_SOURCES}
    ${CLICKHOUSE_GIT_IMPORT_SOURCES}
    ${CLICKHOUSE_ODBC_BRIDGE_SOURCES}
    ${CLICKHOUSE_KEEPER_SOURCES}
    ${CLICKHOUSE_KEEPER_CONVERTER_SOURCES}
    ${CLICKHOUSE_STATIC_FILES_DISK_UPLOADER_SOURCES}
    ${CLICKHOUSE_DWAL_BENCHMARK_SOURCES})

    target_link_libraries(clickhouse-lib
      ${CLICKHOUSE_SERVER_LINK}
      ${CLICKHOUSE_CLIENT_LINK}
      ${CLICKHOUSE_LOCAL_LINK}
      ${CLICKHOUSE_BENCHMARK_LINK}
      ${CLICKHOUSE_COPIER_LINK}
      ${CLICKHOUSE_EXTRACT_FROM_CONFIG_LINK}
      ${CLICKHOUSE_COMPRESSOR_LINK}
      ${CLICKHOUSE_FORMAT_LINK}
      ${CLICKHOUSE_OBFUSCATOR_LINK}
      ${CLICKHOUSE_GIT_IMPORT_LINK}
      ${CLICKHOUSE_ODBC_BRIDGE_LINK}
      ${CLICKHOUSE_KEEPER_LINK}
      ${CLICKHOUSE_KEEPER_CONVERTER_LINK}
      ${CLICKHOUSE_STATIC_FILES_DISK_UPLOADER_LINK})

    target_include_directories(clickhouse-lib
      ${CLICKHOUSE_SERVER_INCLUDE}
      ${CLICKHOUSE_CLIENT_INCLUDE}
      ${CLICKHOUSE_LOCAL_INCLUDE}
      ${CLICKHOUSE_BENCHMARK_INCLUDE}
      ${CLICKHOUSE_COPIER_INCLUDE}
      ${CLICKHOUSE_EXTRACT_FROM_CONFIG_INCLUDE}
      ${CLICKHOUSE_COMPRESSOR_INCLUDE}
      ${CLICKHOUSE_FORMAT_INCLUDE}
      ${CLICKHOUSE_OBFUSCATOR_INCLUDE}
      ${CLICKHOUSE_GIT_IMPORT_INCLUDE}
      ${CLICKHOUSE_ODBC_BRIDGE_INCLUDE}
      ${CLICKHOUSE_KEEPER_INCLUDE}
      ${CLICKHOUSE_KEEPER_CONVERTER_INCLUDE})

    set_target_properties(clickhouse-lib PROPERTIES SOVERSION ${VERSION_MAJOR}.${VERSION_MINOR} VERSION ${VERSION_SO} OUTPUT_NAME proton DEBUG_POSTFIX "")
    install (TARGETS clickhouse-lib LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT proton)
endif()

if (PROTON_SPLIT_BINARY)
    set (CLICKHOUSE_ALL_TARGETS
        proton-server
        proton-client
        proton-local
        proton-benchmark
        proton-extract-from-config
        proton-compressor
        proton-format
        proton-obfuscator
        proton-git-import
        proton-copier
        proton-static-files-disk-uploader
        proton-dwal-benchmark
    )

    if (ENABLE_PROTON_ODBC_BRIDGE)
        list (APPEND CLICKHOUSE_ALL_TARGETS proton-odbc-bridge)
    endif ()

    if (ENABLE_PROTON_LIBRARY_BRIDGE)
        list (APPEND CLICKHOUSE_ALL_TARGETS proton-library-bridge)
    endif ()

    if (ENABLE_PROTON_KEEPER)
        list (APPEND CLICKHOUSE_ALL_TARGETS proton-keeper)
    endif ()

    if (ENABLE_PROTON_KEEPER_CONVERTER)
        list (APPEND CLICKHOUSE_ALL_TARGETS proton-keeper-converter)
    endif ()

    # proton: starts.
    if (ENABLE_PROTON_METASTORE)
        list (APPEND CLICKHOUSE_ALL_TARGETS proton-metastore)
    endif ()
    # proton: ends.

    set_target_properties(${CLICKHOUSE_ALL_TARGETS} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

    add_custom_target (proton-bundle ALL DEPENDS ${CLICKHOUSE_ALL_TARGETS})
    add_custom_target (proton ALL DEPENDS proton-bundle)

    install(PROGRAMS proton-split-helper DESTINATION ${CMAKE_INSTALL_BINDIR} RENAME proton COMPONENT proton)
else ()
    add_executable (proton main.cpp)

    # A library that prevent usage of several functions from libc.
    if (ARCH_AMD64 AND OS_LINUX AND NOT OS_ANDROID)
        set (HARMFUL_LIB harmful)
    endif ()

    target_link_libraries (proton PRIVATE clickhouse_common_io string_utils ${HARMFUL_LIB})
    target_include_directories (proton PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

    if (ENABLE_PROTON_SERVER)
        clickhouse_target_link_split_lib(proton server)
    endif ()
    if (ENABLE_PROTON_CLIENT)
        clickhouse_target_link_split_lib(proton client)
    endif ()
    if (ENABLE_PROTON_LOCAL)
        clickhouse_target_link_split_lib(proton local)
    endif ()
    if (ENABLE_PROTON_BENCHMARK)
        clickhouse_target_link_split_lib(proton benchmark)
    endif ()
    if (ENABLE_PROTON_COPIER)
        clickhouse_target_link_split_lib(proton copier)
    endif ()
    if (ENABLE_PROTON_EXTRACT_FROM_CONFIG)
        clickhouse_target_link_split_lib(proton extract-from-config)
    endif ()
    if (ENABLE_PROTON_COMPRESSOR)
        clickhouse_target_link_split_lib(proton compressor)
    endif ()
    if (ENABLE_PROTON_FORMAT)
        clickhouse_target_link_split_lib(proton format)
    endif ()
    if (ENABLE_PROTON_OBFUSCATOR)
        clickhouse_target_link_split_lib(proton obfuscator)
    endif ()
    if (ENABLE_PROTON_GIT_IMPORT)
        clickhouse_target_link_split_lib(proton git-import)
    endif ()
    if (ENABLE_PROTON_STATIC_FILES_DISK_UPLOADER)
        clickhouse_target_link_split_lib(proton static-files-disk-uploader)
    endif ()
    if (ENABLE_PROTON_KEEPER)
        clickhouse_target_link_split_lib(proton keeper)
    endif()
    if (ENABLE_PROTON_KEEPER_CONVERTER)
        clickhouse_target_link_split_lib(proton keeper-converter)
    endif()
    # proton: starts.
    if (ENABLE_PROTON_METASTORE)
        clickhouse_target_link_split_lib(proton metastore)
    endif()
    # proton: ends.
    if (ENABLE_PROTON_INSTALL)
        clickhouse_target_link_split_lib(proton install)
    endif ()
    if (ENABLE_PROTON_DWAL_BENCHMARK)
        clickhouse_target_link_split_lib(proton dwal-benchmark)
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
    if (ENABLE_PROTON_COPIER)
        add_custom_target (proton-copier ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-copier DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-copier" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-copier)
    endif ()
    if (ENABLE_PROTON_EXTRACT_FROM_CONFIG)
        add_custom_target (proton-extract-from-config ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-extract-from-config DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-extract-from-config" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-extract-from-config)
    endif ()
    if (ENABLE_PROTON_COMPRESSOR)
        add_custom_target (proton-compressor ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-compressor DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-compressor" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-compressor)
    endif ()
    if (ENABLE_PROTON_FORMAT)
        add_custom_target (proton-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-format DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-format" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-format)
    endif ()
    if (ENABLE_PROTON_OBFUSCATOR)
        add_custom_target (proton-obfuscator ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-obfuscator DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-obfuscator" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-obfuscator)
    endif ()
    if (ENABLE_PROTON_GIT_IMPORT)
        add_custom_target (proton-git-import ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-git-import DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-git-import" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-git-import)
    endif ()
    if (ENABLE_PROTON_STATIC_FILES_DISK_UPLOADER)
        add_custom_target (proton-static-files-disk-uploader ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-static-files-disk-uploader DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-static-files-disk-uploader" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-static-files-disk-uploader)
    endif ()
    if (ENABLE_PROTON_KEEPER)
        add_custom_target (proton-keeper ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-keeper DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-keeper" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-keeper)
    endif ()
    if (ENABLE_PROTON_KEEPER_CONVERTER)
        add_custom_target (proton-keeper-converter ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-keeper-converter DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-keeper-converter" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-keeper-converter)
    endif ()
    # proton: starts.
    if (ENABLE_PROTON_METASTORE)
        add_custom_target (proton-metastore ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-metastore DEPENDS proton)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/proton-metastore" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-metastore)
    endif ()
    # proton: ends.
    if (ENABLE_PROTON_DWAL_BENCHMARK)
        add_custom_target (proton-dwal-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink proton proton-dwal-benchmark DEPENDS proton)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/proton-dwal-benchmark DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
        list(APPEND PROTON_BUNDLE proton-dwal-benchmark)
    endif ()

    install (TARGETS proton RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)

    add_custom_target (proton-bundle ALL DEPENDS ${PROTON_BUNDLE})

    if (USE_GDB_ADD_INDEX)
        add_custom_command(TARGET proton POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} proton COMMENT "Adding .gdb-index to proton" VERBATIM)
    endif()

    if (USE_BINARY_HASH)
        add_custom_command(TARGET proton POST_BUILD COMMAND ./proton hash-binary > hash && ${OBJCOPY_PATH} --add-section .note.proton .hash=hash proton COMMENT "Adding .note.proton.hash to proton" VERBATIM)
    endif()
endif ()

if (ENABLE_TESTS)
    set (CLICKHOUSE_UNIT_TESTS_TARGETS unit_tests_dbms)
    add_custom_target (proton-tests ALL DEPENDS ${CLICKHOUSE_UNIT_TESTS_TARGETS})
    add_dependencies(proton-bundle proton-tests)
endif()

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()
