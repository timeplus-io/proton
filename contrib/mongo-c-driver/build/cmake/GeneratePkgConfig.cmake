include_guard(GLOBAL)

include(GNUInstallDirs)

define_property(
    TARGET PROPERTY pkg_config_REQUIRES INHERITED
    BRIEF_DOCS "pkg-config 'Requires:' items"
    FULL_DOCS "Specify 'Requires:' items for the targets' pkg-config file"
    )
define_property(
    TARGET PROPERTY pkg_config_NAME INHERITED
    BRIEF_DOCS "The 'Name' for pkg-config"
    FULL_DOCS "The 'Name' of the pkg-config target"
    )
define_property(
    TARGET PROPERTY pkg_config_DESCRIPTION INHERITED
    BRIEF_DOCS "The 'Description' pkg-config property"
    FULL_DOCS "The 'Description' property to add to a target's pkg-config file"
    )
define_property(
    TARGET PROPERTY pkg_config_VERSION INHERITED
    BRIEF_DOCS "The 'Version' pkg-config property"
    FULL_DOCS "The 'Version' property to add to a target's pkg-config file"
    )
define_property(
    TARGET PROPERTY pkg_config_CFLAGS INHERITED
    BRIEF_DOCS "The 'Cflags' pkg-config property"
    FULL_DOCS "Set a list of options to add to a target's pkg-config file 'Cflags' field"
    )
define_property(
    TARGET PROPERTY pkg_config_INCLUDE_DIRECTORIES INHERITED
    BRIEF_DOCS "Add -I options to the 'Cflags' pkg-config property"
    FULL_DOCS "Set a list of directories that will be added using -I for the 'Cflags' pkg-config field"
    )
define_property(
    TARGET PROPERTY pkg_config_LIBS INHERITED
    BRIEF_DOCS "Add linker options to the 'Libs' pkg-config field"
    FULL_DOCS "Set a list of linker options that will joined in a string for the 'Libs' pkg-config field"
    )

# Given a string, escape any generator-expression-special characters
function(_genex_escape out in)
    # Escape '>'
    string(REPLACE ">" "$<ANGLE-R>" str "${in}")
    # Escape "$"
    string(REPLACE "$" "$<1:$>" str "${str}")
    # Undo the escaping of the dollar for $<ANGLE-R>
    string(REPLACE "$<1:$><ANGLE-R>" "$<ANGLE-R>" str "${str}")
    # Escape ","
    string(REPLACE "," "$<COMMA>" str "${str}")
    # Escape ";"
    string(REPLACE ";" "$<SEMICOLON>" str "${str}")
    set("${out}" "${str}" PARENT_SCOPE)
endfunction()

# Create a generator expression that ensures the given input generator expression
# is evaluated within the context of the named target.
function(_bind_genex_to_target out target genex)
    _genex_escape(escaped "${genex}")
    set("${out}" $<TARGET_GENEX_EVAL:${target},${escaped}> PARENT_SCOPE)
endfunction()

#[==[
Generate a pkg-config .pc file for the Given CMake target, and optionally a
rule to install it::

    generate_pkg_config(
        <target>
        [FILENAME <filename>]
        [LIBDIR <libdir>]
        [INSTALL [DESTINATION <dir>] [RENAME <filename>]]
        [CONDITION <cond>]
    )

The `<target>` must name an existing target. The following options are accepted:

FILENAME <filename>
    - The generated .pc file will have the given `<filename>`. This name *must*
      be only the filename, and not a qualified path. If omitted, the default
      filename is generated based on the target name. If using a multi-config
      generator, the default filename will include the name of the configuration
      for which it was generated.

LIBDIR <libdir>
    - Specify the subdirectory of the install prefix in which the target binary
      will live. If unspecified, uses `CMAKE_INSTALL_LIBDIR`, which comes from
      the GNUInstallDirs module, which has a default of `lib`.

INSTALL [DESTINATION <dir>] [RENAME <filename>]
    - Generate a rule to install the generated pkg-config file. This is better
      than using a `file(INSTALL)` on the generated file directly, since it
      ensures that the installed .pc file will have the correct install prefix
      value. The following additional arguments are also accepted:

      DESTINATION <dir>
        - If provided, specify the *directory* (relative to the install-prefix)
          in which the generated file will be installed. If unspecified, the
          default destination is `<libdir>/pkgconfig`

      RENAME <filename>
        - If provided, set the filename of the installed pkg-config file. If
          unspecified, the top-level `<filename>` will be used. (Note that the
          default top-level `<filename>` will include the configuration type
          when built/installed using a multi-config generator!)

CONDITION <cond>
    - The file will only be generated/installed if the condition `<cond>`
      results in the string "1" after evaluating generator expressions.

All named parameters accept generator expressions.

]==]
function(mongo_generate_pkg_config target)
    list(APPEND CMAKE_MESSAGE_CONTEXT "mongo_generate_pkg_config" "${target}")
    # Collect some target properties:
    # The name:
    _genex_escape(proj_name "${PROJECT_NAME}")
    _genex_escape(proj_desc "${PROJECT_DESCRIPTION}")
    set(tgt_name $<TARGET_PROPERTY:pkg_config_NAME>)
    set(tgt_version $<TARGET_PROPERTY:pkg_config_VERSION>)
    set(tgt_desc $<TARGET_PROPERTY:pkg_config_DESCRIPTION>)
    string(CONCAT gx_name
        $<IF:$<STREQUAL:,${tgt_name}>,
             ${proj_name},
             ${tgt_name}>)
    # Version:
    string(CONCAT gx_version
        $<IF:$<STREQUAL:,${tgt_version}>,
             ${PROJECT_VERSION},
             ${tgt_version}>)
    # Description:
    string(CONCAT gx_desc
        $<IF:$<STREQUAL:,${tgt_desc}>,
             ${proj_desc},
             ${tgt_desc}>)

    # Parse and validate arguments:
    cmake_parse_arguments(PARSE_ARGV 1 ARG "" "FILENAME;LIBDIR;CONDITION" "INSTALL")

    # Compute the default FILENAME
    if(NOT DEFINED ARG_FILENAME)
        # No filename given. Pick a default:
        if(DEFINED CMAKE_CONFIGURATION_TYPES)
            # Multi-conf: We may want to generate more than one, so qualify the
            # filename with the configuration type:
            set(ARG_FILENAME "$<TARGET_FILE_BASE_NAME:${target}>-$<LOWER_CASE:$<CONFIG>>.pc")
        else()
            # Just generate a file based on the basename of the target:
            set(ARG_FILENAME "$<TARGET_FILE_BASE_NAME:${target}>.pc")
        endif()
    endif()
    message(DEBUG "FILENAME: ${ARG_FILENAME}")

    # The defalut CONDITION is just "1" (true)
    if(NOT DEFINED ARG_CONDITION)
        set(ARG_CONDITION 1)
    endif()
    message(DEBUG "CONDITION: ${ARG_CONDITION}")
    _bind_genex_to_target(gx_cond ${target} "${ARG_CONDITION}")

    # The default LIBDIR comes from GNUInstallDirs.cmake
    if(NOT ARG_LIBDIR)
        set(ARG_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
    endif()
    message(DEBUG "LIBDIR: ${ARG_LIBDIR}")
    _bind_genex_to_target(gx_libdir ${target} "${ARG_LIBDIR}")

    # Evaluate the filename genex in the context of the target:
    _bind_genex_to_target(gx_filename ${target} "${ARG_FILENAME}")
    if(IS_ABSOLUTE "${ARG_FILENAME}")
        set(gx_output "${gx_filename}")
    else()
        get_filename_component(gx_output "${CMAKE_CURRENT_BINARY_DIR}/${gx_filename}" ABSOLUTE)
    endif()
    message(DEBUG "Generating build-tree file: ${gx_output}")

    # Generate the content of the file:
    _generate_pkg_config_content(content
        NAME "${gx_name}"
        VERSION "${gx_version}"
        DESCRIPTION "${gx_desc}"
        PREFIX "%INSTALL_PLACEHOLDER%"
        LIBDIR "${gx_libdir}"
        GENEX_TARGET "${target}"
        )
    _bind_genex_to_target(gx_content ${target} "${content}")
    string(REPLACE "%INSTALL_PLACEHOLDER%" "${CMAKE_INSTALL_PREFIX}" gx_with_prefix "${gx_content}")
    # Now, generate the file:
    file(GENERATE
         OUTPUT "${gx_output}"
         CONTENT "${gx_with_prefix}"
         CONDITION "${gx_cond}")
    if(NOT "INSTALL" IN_LIST ARGN)
        # Nothing more to do here.
        message(DEBUG "(Not installing)")
        return()
    endif()

    # Installation handling:
    # Use file(GENERATE) to generate a temporary file to be picked up at install-time.
    # (For some reason, injecting the content directly into install(CODE) fails in corner cases)
    set(gx_tmpfile "${CMAKE_CURRENT_BINARY_DIR}/_pkgconfig/${target}-$<LOWER_CASE:$<CONFIG>>-for-install.txt")
    message(DEBUG "Generate for-install: ${gx_tmpfile}")
    file(GENERATE OUTPUT "${gx_tmpfile}"
         CONTENT "${gx_content}"
         CONDITION "${gx_cond}")
    # Parse the install args that we will inspect:
    cmake_parse_arguments(inst "" "DESTINATION;RENAME" "" ${ARG_INSTALL})
    if(NOT DEFINED inst_DESTINATION)
        # Install based on the libdir:
        set(inst_DESTINATION "${gx_libdir}/pkgconfig")
    endif()
    if(NOT DEFINED inst_RENAME)
        set(inst_RENAME "${ARG_FILENAME}")
    endif()
    message(DEBUG "INSTALL DESTINATION: ${inst_DESTINATION}")
    message(DEBUG "INSTALL RENAME: ${inst_RENAME}")
    # install(CODE) will write a simple temporary file:
    set(inst_tmp "${CMAKE_CURRENT_BINARY_DIR}/${target}-pkg-config-tmp.txt")
    _genex_escape(esc_cond "${ARG_CONDITION}")
    string(CONFIGURE [==[
        $<@gx_cond@:
            # Installation of pkg-config for target @target@
            message(STATUS "Generating pkg-config file: @inst_RENAME@")
            file(READ [[@gx_tmpfile@]] content)
            # Insert the install prefix:
            string(REPLACE "%INSTALL_PLACEHOLDER%" "${CMAKE_INSTALL_PREFIX}" content "${content}")
            # Write it before installing again:
            file(WRITE [[@inst_tmp@]] "${content}")
        >
        $<$<NOT:@gx_cond@>:
            # Installation was disabled for this generation.
            message(STATUS "Skipping install of file [@inst_RENAME@]: Disabled by CONDITION “@esc_cond@”")
        >
    ]==] code @ONLY)
    install(CODE "${code}")
    _bind_genex_to_target(gx_dest ${target} "${inst_DESTINATION}")
    if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.20")
        _bind_genex_to_target(gx_rename ${target} "${inst_RENAME}")
    else()
        # Note: CMake 3.20 is required for using generator expresssions in install(RENAME).
        # if we are older than that, just treat RENAME as a plain value.
        set(gx_rename "${inst_RENAME}")
    endif()
    # Wrap the filename to install with the same condition used to generate it. If the condition
    # is not met, then the FILES list will be empty, and nothing will be installed.
    install(FILES "$<${gx_cond}:${inst_tmp}>"
            DESTINATION ${gx_dest}
            RENAME ${gx_rename}
            ${inst_UNPARSED_ARGUMENTS})
endfunction()

# Generates the actual content of a .pc file.
function(_generate_pkg_config_content out)
    cmake_parse_arguments(PARSE_ARGV 1 ARG "" "PREFIX;NAME;VERSION;DESCRIPTION;GENEX_TARGET;LIBDIR" "")
    if(ARG_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "Unknown arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif()
    set(content)
    string(APPEND content
        "# pkg-config .pc file generated by CMake ${CMAKE_VERSION} for ${ARG_NAME}-${ARG_VERSION}. DO NOT EDIT!\n"
        "prefix=${ARG_PREFIX}\n"
        "exec_prefix=\${prefix}\n"
        "libdir=\${exec_prefix}/${gx_libdir}\n"
        "\n"
        "Name: ${ARG_NAME}\n"
        "Description: ${ARG_DESCRIPTION}\n"
        "Version: ${ARG_VERSION}"
        )
    # Add Requires:
    set(requires_joiner "\nRequires: ")
    set(gx_requires $<GENEX_EVAL:$<TARGET_PROPERTY:pkg_config_REQUIRES>>)
    set(has_requires $<NOT:$<STREQUAL:,${gx_requires}>>)
    string(APPEND content "$<${has_requires}:${requires_joiner}$<JOIN:${gx_requires},${requires_joiner}>>\n")
    string(APPEND content "\n")
    # Add "Libs:"
    set(libs)
    # Link options:
    set(gx_libs
        "-L\${libdir}"
        "-l$<TARGET_PROPERTY:OUTPUT_NAME>"
        $<GENEX_EVAL:$<TARGET_PROPERTY:pkg_config_LIBS>>
        $<REMOVE_DUPLICATES:$<TARGET_PROPERTY:INTERFACE_LINK_OPTIONS>>
        )

    # XXX: Could we define a genex that can transform the INTERFACE_LINK_LIBRARIES to a list of
    #      pkg-config-compatible "-l"-flags? That would remove the need to populate pkg_config_LIBS
    #      manually, and instead rely on target properties to handle transitive dependencies.
    string(APPEND libs "$<JOIN:${gx_libs}, >")

    # Cflags:
    set(cflags)
    set(gx_flags
        $<REMOVE_DUPLICATES:$<GENEX_EVAL:$<TARGET_PROPERTY:pkg_config_CFLAGS>>>
        $<REMOVE_DUPLICATES:$<TARGET_PROPERTY:INTERFACE_COMPILE_OPTIONS>>
        )
    string(APPEND cflags "$<JOIN:${gx_flags}, >")
    # Definitions:
    set(gx_defs $<REMOVE_DUPLICATES:$<TARGET_PROPERTY:INTERFACE_COMPILE_DEFINITIONS>>)
    set(has_defs $<NOT:$<STREQUAL:,${gx_defs}>>)
    set(def_joiner " -D")
    string(APPEND cflags $<${has_defs}:${def_joiner}$<JOIN:${gx_defs},${def_joiner}>>)
    # Includes:
    set(gx_inc $<GENEX_EVAL:$<TARGET_PROPERTY:pkg_config_INCLUDE_DIRECTORIES>>)
    set(gx_inc "$<REMOVE_DUPLICATES:${gx_inc}>")
    set(gx_abs_inc "$<FILTER:${gx_inc},INCLUDE,^/>")
    set(gx_rel_inc "$<FILTER:${gx_inc},EXCLUDE,^/>")
    set(has_abs_inc $<NOT:$<STREQUAL:,${gx_abs_inc}>>)
    set(has_rel_inc $<NOT:$<STREQUAL:,${gx_rel_inc}>>)
    string(APPEND cflags $<${has_rel_inc}: " -I\${prefix}/"
                            $<JOIN:${gx_rel_inc}, " -I\${prefix}/" >>
                         $<${has_abs_inc}: " -I"
                            $<JOIN:${gx_abs_inc}, " -I" >>)
    string(APPEND content "Libs: ${libs}\n")
    string(APPEND content "Cflags: ${cflags}\n")
    set("${out}" "${content}" PARENT_SCOPE)
endfunction()
