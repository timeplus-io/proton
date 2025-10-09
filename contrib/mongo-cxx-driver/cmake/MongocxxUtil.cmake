# Define and link a form of the mongocxx library
#
# This function requires the following variables to be defined in its parent scope:
# - mongocxx_sources
# - libmongoc_target
# - libmongoc_definitions
# - libmongoc_definitions
function(mongocxx_add_library TARGET OUTPUT_NAME LINK_TYPE)
    add_library(${TARGET} ${LINK_TYPE}
        ${mongocxx_sources}
    )

    # Full ABI tag string to append to library output name.
    # The value is determined at generator-time when using a multi-config generator.
    # Otherwise, the value is determined at configure-time.
    set(abi_tag "")

    # ABI tag and properties.
    if(1)
        # Many ABI tag fields are inherited from bsoncxx (must be consistent).
        if(BSONCXX_BUILD_SHARED)
            set(bsoncxx_target bsoncxx_shared)
        else()
            set(bsoncxx_target bsoncxx_static)
        endif()

        # ABI version number. Only necessary for shared library targets.
        if(LINK_TYPE STREQUAL "SHARED")
            set(soversion _noabi)
            set_target_properties(${TARGET} PROPERTIES SOVERSION ${soversion})
            string(APPEND abi_tag "-v${soversion}")
        endif()

        # Build type (same as bsoncxx):
        # - 'd' for debug.
        # - 'r' for release (including RelWithDebInfo and MinSizeRel).
        # - 'u' for unknown (e.g. to allow user-defined configurations).
        # Compatibility is handled via CMake's IMPORTED_CONFIGURATIONS rather than interface properties.
        string(APPEND abi_tag "-$<IF:$<CONFIG:Debug>,d,$<IF:$<OR:$<CONFIG:Release>,$<CONFIG:RelWithDebInfo>,$<CONFIG:MinSizeRel>>,r,u>>")

        # Link type with libmongoc. Inherit from bsoncxx.
        if(1)
            get_target_property(mongoc_link_type ${bsoncxx_target} INTERFACE_BSONCXX_ABI_TAG_MONGOC_LINK_TYPE)

            set_target_properties(${TARGET} PROPERTIES
                BSONCXX_ABI_TAG_MONGOC_LINK_TYPE ${mongoc_link_type}
                INTERFACE_BSONCXX_ABI_TAG_MONGOC_LINK_TYPE ${mongoc_link_type}
            )

            string(APPEND abi_tag "${mongoc_link_type}")
        endif()

        # Library used for C++17 polyfills. Inherit from bsoncxx.
        if(1)
            get_target_property(polyfill ${bsoncxx_target} INTERFACE_BSONCXX_ABI_TAG_POLYFILL_LIBRARY)

            set_target_properties(${TARGET} PROPERTIES
                BSONCXX_ABI_TAG_POLYFILL_LIBRARY ${polyfill}
                INTERFACE_BSONCXX_ABI_TAG_POLYFILL_LIBRARY ${polyfill}
            )

            string(APPEND abi_tag "${polyfill}")
        endif()

        # MSVC-specific ABI tag suffixes. Inherit from bsoncxx.
        if(MSVC)
            get_target_property(vs_suffix ${bsoncxx_target} BSONCXX_ABI_TAG_VS_SUFFIX)
            set_target_properties(${TARGET} PROPERTIES
                BSONCXX_ABI_TAG_VS_SUFFIX ${vs_suffix}
                INTERFACE_BSONCXX_ABI_TAG_VS_SUFFIX ${vs_suffix}
            )

            string(APPEND abi_tag "${vs_suffix}")
        endif()
    endif()

    set_target_properties(${TARGET} PROPERTIES
        VERSION ${MONGOCXX_VERSION}
        DEFINE_SYMBOL MONGOCXX_EXPORTS
    )

    if(ENABLE_ABI_TAG_IN_LIBRARY_FILENAMES)
        set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME ${OUTPUT_NAME}${abi_tag})
    else()
        set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME ${OUTPUT_NAME})
    endif()

    if(LINK_TYPE STREQUAL "SHARED")
        set_target_properties(${TARGET} PROPERTIES
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
        )
    endif()

    if(LINK_TYPE STREQUAL "STATIC")
        target_compile_definitions(${TARGET} PUBLIC MONGOCXX_STATIC)
    endif()

    target_link_libraries(${TARGET} PRIVATE ${libmongoc_target})
    target_include_directories(${TARGET} PRIVATE ${libmongoc_include_directories})
    target_include_directories(
        ${TARGET}
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/mongocxx/v_noabi>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/lib/mongocxx/v_noabi>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/lib>
        $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/lib/mongocxx/v_noabi>
        $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/lib>
    )
    target_compile_definitions(${TARGET} PRIVATE ${libmongoc_definitions})
endfunction(mongocxx_add_library)
