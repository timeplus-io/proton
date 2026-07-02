macro(proton_split_debug_symbols)
    set(oneValueArgs TARGET DESTINATION_DIR BINARY_PATH)

    cmake_parse_arguments(STRIP "" "${oneValueArgs}" "" ${ARGN})

    if (NOT DEFINED STRIP_TARGET)
        message(FATAL_ERROR "A target name must be provided for stripping binary")
    endif()

    if (NOT DEFINED STRIP_BINARY_PATH)
        message(FATAL_ERROR "A binary path name must be provided for stripping binary")
    endif()

    if (NOT DEFINED STRIP_DESTINATION_DIR)
        message(FATAL_ERROR "Destination directory for stripped binary must be provided")
    endif()

    if(APPLE)
        # macOS: cp + in-place strip. Mach-O `strip` split-debug tooling
        # is treated as out-of-scope for this PR, so preserve prior behavior.
        add_custom_command(TARGET ${STRIP_TARGET} POST_BUILD
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/lib/debug/bin"
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/bin"
            COMMAND cp "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            # Splits debug symbols into separate file, leaves the binary untouched:
            COMMAND "${OBJCOPY_PATH}" --only-keep-debug "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}" "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND chmod 0644 "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND "${STRIP_PATH}" --remove-section=.comment --remove-section=.note "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            # Associate stripped binary with debug symbols:
            COMMAND "${OBJCOPY_PATH}" --add-gnu-debuglink "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMENT "Stripping proton binary" VERBATIM
        )
    else()
        # Linux: atomic strip (`<src> -o <dst>`) — strip reads the original
        # and writes the stripped output directly to the destination, so a
        # failed strip can't leave a half-copied binary behind. Mirrors
        # upstream ClickHouse's split_debug_symbols flow.
        #
        # `--strip-debug` (vs strip's default `--strip-all`) keeps `.symtab`
        # so SymbolIndex can resolve function names in crash backtraces when
        # the `-proton-debug` package is not installed — required now that
        # `-Wl,--no-export-dynamic` has shrunk `.dynsym` (see upstream
        # PR ClickHouse/ClickHouse#47475). `.proton.hash` is a non-debug
        # section, so `--keep-section` is no longer needed.
        #
        # `.note` and `.comment` are removed in line with Debian's stripping
        # policy (https://www.debian.org/doc/debian-policy/ch-files.html).
        add_custom_command(TARGET ${STRIP_TARGET} POST_BUILD
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/lib/debug/bin"
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/bin"
            COMMAND "${OBJCOPY_PATH}" --only-keep-debug "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND chmod 0644 "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND "${STRIP_PATH}" --strip-debug --remove-section=.comment --remove-section=.note "${STRIP_BINARY_PATH}" -o "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMAND "${OBJCOPY_PATH}" --add-gnu-debuglink "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMENT "Stripping proton binary" VERBATIM
        )
    endif()

    install(PROGRAMS ${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET} DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT proton)
    install(FILES ${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug DESTINATION ${CMAKE_INSTALL_LIBDIR}/debug/${CMAKE_INSTALL_FULL_BINDIR} COMPONENT proton)
endmacro()


# Same as proton_split_debug_symbols, but does not add install() rules.
macro(proton_split_debug_symbols_without_install)
    set(oneValueArgs TARGET DESTINATION_DIR BINARY_PATH)

    cmake_parse_arguments(STRIP "" "${oneValueArgs}" "" ${ARGN})

    if (NOT DEFINED STRIP_TARGET)
        message(FATAL_ERROR "A target name must be provided for stripping binary")
    endif()

    if (NOT DEFINED STRIP_BINARY_PATH)
        message(FATAL_ERROR "A binary path name must be provided for stripping binary")
    endif()

    if (NOT DEFINED STRIP_DESTINATION_DIR)
        message(FATAL_ERROR "Destination directory for stripped binary must be provided")
    endif()

    if(APPLE)
        add_custom_command(TARGET ${STRIP_TARGET} POST_BUILD
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/lib/debug/bin"
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/bin"
            COMMAND cp "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMAND "${OBJCOPY_PATH}" --only-keep-debug "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}" "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND chmod 0644 "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND "${STRIP_PATH}" --remove-section=.comment --remove-section=.note "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMAND "${OBJCOPY_PATH}" --add-gnu-debuglink "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMENT "Stripping proton binary (no install)" VERBATIM
        )
    else()
        # Same atomic `--strip-debug` + `-o` flow as proton_split_debug_symbols;
        # see that macro for rationale.
        add_custom_command(TARGET ${STRIP_TARGET} POST_BUILD
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/lib/debug/bin"
            COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/bin"
            COMMAND "${OBJCOPY_PATH}" --only-keep-debug "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND chmod 0644 "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
            COMMAND "${STRIP_PATH}" --strip-debug --remove-section=.comment --remove-section=.note "${STRIP_BINARY_PATH}" -o "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMAND "${OBJCOPY_PATH}" --add-gnu-debuglink "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
            COMMENT "Stripping proton binary (no install)" VERBATIM
        )
    endif()
endmacro()


macro(proton_make_empty_debug_info_for_nfpm)
    set(oneValueArgs TARGET DESTINATION_DIR)
    cmake_parse_arguments(EMPTY_DEBUG "" "${oneValueArgs}" "" ${ARGN})

    if (NOT DEFINED EMPTY_DEBUG_TARGET)
        message(FATAL_ERROR "A target name must be provided for stripping binary")
    endif()

    if (NOT DEFINED EMPTY_DEBUG_DESTINATION_DIR)
        message(FATAL_ERROR "Destination directory for empty debug must be provided")
    endif()

    add_custom_command(TARGET ${EMPTY_DEBUG_TARGET} POST_BUILD
        COMMAND mkdir -p "${EMPTY_DEBUG_DESTINATION_DIR}/lib/debug"
        COMMAND touch "${EMPTY_DEBUG_DESTINATION_DIR}/lib/debug/${EMPTY_DEBUG_TARGET}.debug"
        COMMENT "Adding empty debug info for NFPM" VERBATIM
    )

    install(FILES "${EMPTY_DEBUG_DESTINATION_DIR}/lib/debug/${EMPTY_DEBUG_TARGET}.debug" DESTINATION "${CMAKE_INSTALL_LIBDIR}/debug/${CMAKE_INSTALL_FULL_BINDIR}" COMPONENT proton)
endmacro()
