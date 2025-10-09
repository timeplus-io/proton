#[==[
	Define a build-time static library target whose compilation asserts that
    header files are properly guarding config macros using inclusion of
    prelude/postlude headers.

    Usage:
	
	    add_macro_guard_test(
	        PROJECT_NAME <name>
	        PROJECT_TEST_PROPERTIES_TARGET <target>
	        INCLUDE_PATTERNS [pattern...]
	        [EXCLUDE_REGEXES [pattern...]]
	        GUARDED_MACROS [macro...]
	    )
	
	PROJECT_NAME
	    Either "bsoncxx" or "mongocxx".
	
	PROJECT_TEST_PROPERTIES_TARGET
	    Either the bsoncxx_test_properties or mongocxx_test_properties target.
	
	GUARDED_MACROS
	    List of macros that should be guarded by prelude/postlude headers.
	
	INCLUDE_PATTERNS
	    List of regex filters for headers to be added to the tests. Must be
        relative to PROJECT_SOURCE_DIR.
	
	EXCLUDE_REGEXES
	    List of regex filters for headers to be excluded. Applied after
        INCLUDE_PATTERNS.
]==]
function(add_macro_guard_test)
    set(opt_args "")
    set(single_args "PROJECT_NAME;PROJECT_TEST_PROPERTIES_TARGET")
    set(multi_args "GUARDED_MACROS;INCLUDE_PATTERNS;EXCLUDE_REGEXES")

    cmake_parse_arguments(PARSED "${opt_args}" "${single_args}" "${multi_args}" ${ARGN})

    if(NOT "${PARSED_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "unrecognized argument: ${PARSED_UNPARSED_ARGUMENTS}")
    endif()

    foreach(required_arg PROJECT_NAME PROJECT_TEST_PROPERTIES_TARGET GUARDED_MACROS INCLUDE_PATTERNS)
        if("${required_arg}" IN_LIST PARSED_KEYWORDS_MISSING_VALUES)
            message(FATAL_ERROR "missing value for required argument ${required_arg}")
        endif()
    endforeach()

    list(TRANSFORM PARSED_INCLUDE_PATTERNS PREPEND "${PROJECT_SOURCE_DIR}/")
    file(GLOB_RECURSE GUARDED_HEADERS
        LIST_DIRECTORIES false
        RELATIVE ${PROJECT_SOURCE_DIR}
        ${PARSED_INCLUDE_PATTERNS}
    )

    foreach(filter ${PARSED_EXCLUDE_REGEXES})
        list(FILTER GUARDED_HEADERS EXCLUDE REGEX "${filter}")
    endforeach()

    set(MACRO_GUARD_TEST_PRELUDE "")

    # Check and set initial state.
    foreach(macro ${PARSED_GUARDED_MACROS})
        string(APPEND MACRO_GUARD_TEST_PRELUDE
            "#if defined(${macro})\n"
            "#error \"${macro} is already defined\"\n"
            "#endif\n"
            "#define ${macro} macro guard test\n"
            "\n"
        )
    endforeach()

    # Implement as recursive algorithm for C++11 compatibility.
    string(APPEND MACRO_GUARD_TEST_PRELUDE
        "static constexpr bool compare_equal(const char* lhs, const char* rhs) {\n"
        "    return (*lhs == *rhs) && (*lhs == '\\0' || compare_equal(lhs + 1, rhs + 1));\n"
        "}\n"
        "\n"
        "static_assert(compare_equal(\"abc\", \"abc\"), \"compare_equal() sanity check failed\");\n"
        "static_assert(!compare_equal(\"abc\", \"def\"), \"compare_equal() sanity check failed\");\n"
        "\n"
        "#define _TO_STR(x) #x\n"
        "#define TO_STR(x) _TO_STR(x)\n"
        "\n"
    )

    add_library(test_${PARSED_PROJECT_NAME}_macro_guards STATIC EXCLUDE_FROM_ALL)
    target_link_libraries(test_${PARSED_PROJECT_NAME}_macro_guards PRIVATE ${PARSED_PROJECT_TEST_PROPERTIES_TARGET})

    # Test each header individually.
    foreach(header ${GUARDED_HEADERS})
        set(MACRO_GUARD_TEST "${MACRO_GUARD_TEST_PRELUDE}")

        # Strip the subdir.
        string(REGEX REPLACE "^(include|lib|test)/(.*)$" "\\1" subdir "${header}")
        string(REGEX REPLACE "^(include|lib|test)/(.*)$" "\\2" header ${header})

        # Apply include prefix to test headers.
        if("${subdir}" STREQUAL "test")
            set(relheader "${PARSED_PROJECT_NAME}/test/${header}")
        else()
            set(relheader "${header}")
        endif()

        # CXX-2770: workaround missing postlude header includes.
        string(TOUPPER "${PARSED_PROJECT_NAME}" project_name_upper)
        string(APPEND MACRO_GUARD_TEST "#define ${project_name_upper}_TEST_MACRO_GUARDS_FIX_MISSING_POSTLUDE\n\n")

        # The include directive.
        string(APPEND MACRO_GUARD_TEST "#include <${relheader}>\n\n")

        # Test all guarded macros have been properly restored.
        foreach(macro ${PARSED_GUARDED_MACROS})
            string(APPEND MACRO_GUARD_TEST
                "static_assert(\n"
                "    compare_equal(TO_STR(${macro}),\"macro guard test\"),\n"
                "    \"${macro} was not correctly restored by <${relheader}>\"\n"
                ");\n"
                "\n"
            )
        endforeach()

        # e.g. bsoncxx/v_noabi/bsoncxx/document/view.hpp -> bsoncxx-v_noabi-bsoncxx-document-view.cpp
        string(REPLACE "/" "-" test_name "${header}")
        string(REGEX REPLACE "^(.*)\\.(hh|hpp)$" "\\1" test_name "${test_name}")

        # e.g. macro_guards/(include|lib|test)/bsoncxx-v_noabi-bsoncxx-document-view.cpp
        configure_file(test_macro_guards.cpp.in macro_guards/${subdir}/${test_name}.cpp)

        target_sources(test_${PARSED_PROJECT_NAME}_macro_guards PRIVATE
            ${CMAKE_CURRENT_BINARY_DIR}/macro_guards/${subdir}/${test_name}.cpp
        )
    endforeach()
endfunction()
