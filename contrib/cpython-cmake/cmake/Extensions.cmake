# This function adds a python extension to the buildsystem.
#
# Usage:
#
# add_python_extension(
#     extension_name
#     SOURCES source1.c source2.c ...
#     [ DEFINITIONS define1 define2 ... ]
#     [ LIBRARIES lib1 lib2 ... ]
#     [ INCLUDEDIRS dir1 dir2 ... ]
# )
#
# extension_name: the name of the library
# SOURCES:     a list of filenames realtive to the Modules/ directory that make
#              up this extension.
#              here evaluate to true.  You should include any variables you use
#              in the LIBRARIES and INCLUDEDIRS sections.
# DEFINITIONS: an optional list of definitions to pass to the compiler while
#              building this module.  Do not include the -D prefix.
# LIBRARIES:   an optional list of additional libraries.
# INCLUDEDIRS: an optional list of additional include directories.


function(add_python_extension name)
    set(options )
    set(oneValueArgs)
    set(multiValueArgs SOURCES DEFINITIONS LIBRARIES INCLUDEDIRS)
    cmake_parse_arguments(ADD_PYTHON_EXTENSION
        "${options}"
        "${oneValueArgs}"
        "${multiValueArgs}"
        ${ARGN}
        )

    # Remove _ from the beginning of the name.
    string(REGEX REPLACE "^_" "" pretty_name "${name}")

    # Add a prefix to the target name so it doesn't clash with any system
    # libraries that we might want to link against (eg. readline)
    set(target_name libpython_extension_${pretty_name})

    # Callers to this function provide source files relative to the Modules/
    # directory.  We need to get absolute paths for them all.
    set(absolute_sources "")
    foreach(source ${ADD_PYTHON_EXTENSION_SOURCES})
        get_filename_component(ext ${source} EXT)

        # Treat assembler sources differently
        if(ext STREQUAL ".S")
            set_source_files_properties(Modules/${source} PROPERTIES LANGUAGE ASM)
        endif()
        set(absolute_src ${source})
        if(NOT IS_ABSOLUTE ${source})
            set(absolute_src ${CPYTHON_SOURCE_DIR}/Modules/${source})
        endif()
        list(APPEND absolute_sources ${absolute_src})
    endforeach()

    add_library(${target_name} STATIC ${absolute_sources})


    set(DEFAULT_LIBPYTHON_HEADER)
    list(APPEND DEFAULT_LIBPYTHON_HEADER
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
    )

    target_compile_definitions(${target_name}
        PUBLIC
            Py_NO_ENABLE_SHARED
    )

    target_include_directories(${target_name}
        PUBLIC
            ${DEFAULT_LIBPYTHON_HEADER}
            ${ADD_PYTHON_EXTENSION_INCLUDEDIRS}
    )

    target_link_libraries(${target_name} PUBLIC ${ADD_PYTHON_EXTENSION_LIBRARIES})

    if(PY_VERSION VERSION_GREATER_EQUAL "3.8")
        list(APPEND ADD_PYTHON_EXTENSION_DEFINITIONS Py_BUILD_CORE_MODULE)
    endif()

    if(ADD_PYTHON_EXTENSION_DEFINITIONS)
        set_target_properties(${target_name} PROPERTIES
            COMPILE_DEFINITIONS "${ADD_PYTHON_EXTENSION_DEFINITIONS}")
    endif()

    # Create the parts of config.c for platform-specific and user-controlled
    # builtin modules.
    get_property(config_inits GLOBAL PROPERTY config_inits)
    set(new_config_inits "${config_inits} extern PyObject* PyInit_${name}(void);\n")
    set_property(GLOBAL PROPERTY config_inits ${new_config_inits})

    get_property(config_entries GLOBAL PROPERTY config_entries)
    set(new_config_entries "${config_entries}    {\"${name}\", PyInit_${name}},\n")
    set_property(GLOBAL PROPERTY config_entries ${new_config_entries})

    set_property(GLOBAL APPEND PROPERTY extension_libraries ${target_name})
    message(STATUS "Adding extension: ${target_name}")
endfunction()
