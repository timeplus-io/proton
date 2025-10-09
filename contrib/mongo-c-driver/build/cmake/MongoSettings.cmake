include_guard(DIRECTORY)

#[[ Bool: Set to TRUE if the environment variable MONGODB_DEVELOPER is a true value ]]
set(MONGODB_DEVELOPER FALSE)

# Detect developer mode:
set(_is_dev "$ENV{MONGODB_DEVELOPER}")
if(_is_dev)
    message(STATUS "Enabling MONGODB_DEVELOPER 🍃")
    set(MONGODB_DEVELOPER TRUE)
endif()

#[==[
Define a new configure-time build setting::
    mongo_setting(
        <name> <doc>
        [TYPE <BOOL|PATH|FILEPATH|STRING>]
        [DEFAULT [[DEVEL] [VALUE <value> | EVAL <code>]] ...]
        [OPTIONS [<opts> ...]]
        [VALIDATE [CODE <code>]]
        [VISIBLE_IF <cond>]
        [ADVANCED]
    )

The `<name>` will be the name of the setting, while `<doc>` will be the
documentation string shown to the user. Newlines in the doc string will be
replaced with single spaces. If no other arguments are provided, the default
`TYPE` will be `STRING`, and the DEFAULT value will be an empty string. If the
previous cached value is AUTO, and AUTO is not listed in OPTIONS, then the cache
value will be cleared and reset to the default value.

Package maintainers Note: Setting a variable `<name>-FORCE` to TRUE will make
this function a no-op.

TYPE <BOOL|PATH|FILEPATH|STRING>
    - Sets the type for the generated cache variable. If the type is BOOL, this
      call will validate that the setting is a valid boolean value.

OPTIONS [<opts> [...]]
    - Specify the valid set of values available for this setting. This will set
      the STRINGS property on the cache variable and add an information message
      to the doc string. This call will also validate that the setting's value
      is one of these options, failing with an error if it is not.

DEFAULT [[DEVEL] [VALUE <value> | EVAL <code>]]
        [...]
    - Specify the default value(s) of the generated variable. If given VALUE,
      then `<value>` will be used as the default, otherwise if given EVAL,
      `<code>` will be executed and is expected to define a variable DEFAULT
      that will contain the default value. An optional DEVEL qualifier may be
      given before a default value specifier. If both qualified and unqualified
      defaults are given, the unqualified default must appear first in the
      argument list.
    - If MONGODB_DEVELOPER is not true, then the non-qualified default will be
      used. (If no non-qualified defaults are provided, then the default value
      is an empty string.)
    - Otherwise, If DEVEL defaults are provided and MONGODB_DEVELOPER is true,
      then the DEVEL defaults will be used.

VALIDATE [CODE <code>]
    - If specified, then `<code>` will be evaluated after the setting value is
      defined. `<code>` may issue warnings and errors about the value of the
      setting.

ADVANCED
    - If specified, the cache variable will be marked as an advanced setting

VISIBLE_IF <cond>
    - If specified, then `<cond>` should be a quoted CMake condition (e.g.
      [[FOO AND NOT BAR]]). If the condition evaluates to a false value, then
      the setting will be hidden from the user. NOTE that the setting will still
      retain its original value and be available as a variable in the CMake
      code!

]==]
function(mongo_setting setting_NAME setting_DOC)
    list(APPEND CMAKE_MESSAGE_CONTEXT mongo setting "${setting_NAME}")
    # Allow bypassing this code:
    set(force "${${setting_NAME}-FORCE}")
    if(force)
        return()
    endif()

    cmake_parse_arguments(
        PARSE_ARGV 2 setting
        "ADVANCED"
        "TYPE;VISIBLE_IF"
        "OPTIONS;DEFAULT;VALIDATE")
    # Check for unknown arguments:
    foreach(arg IN LISTS setting_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Unrecognized argument: “${arg}”")
    endforeach()
    if(setting_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "Unrecognized arguments (see above)")
    endif()

    # By default, settings are strings:
    if(NOT DEFINED setting_TYPE)
        set(setting_TYPE STRING)
    endif()

    # More arg validation:
    if(setting_TYPE STREQUAL "BOOL")
        if(DEFINED setting_OPTIONS)
            message(FATAL_ERROR [["OPTIONS" cannot be specified with type "BOOL"]])
        endif()
    endif()

    # Normalize the doc string for easier writing of doc strings at call sites:
    string(REGEX REPLACE "\n[ ]*" " " doc "${setting_DOC}")
    # Update the doc string with options:
    if(DEFINED setting_OPTIONS)
        string(REPLACE ";" ", " opts "${setting_OPTIONS}")
        string(APPEND doc " (Options: ${opts})")
    endif()

    # Get the default option value:
    unset(DEFAULT)
    if(DEFINED setting_DEFAULT)
        _mongo_compute_default(DEFAULT "${setting_DEFAULT}")
    endif()

    if(DEFINED DEFAULT)
        # Add that to the doc message:
        string(APPEND doc " (Default is “${DEFAULT}”)")
        # Check that the default is actually a valid option:
        if(DEFINED setting_OPTIONS AND NOT DEFAULT IN_LIST setting_OPTIONS)
            message(AUTHOR_WARNING "${setting_NAME}: Setting's default value is “${DEFAULT}”, which is not one of the provided setting options (${opts})")
        endif()

        # Reset "AUTO" values to the default
        if(NOT "AUTO" IN_LIST setting_OPTIONS AND "$CACHE{${setting_NAME}}" STREQUAL "AUTO")
            message(WARNING "Replacing old ${setting_NAME}=“AUTO” with the new default value ${setting_NAME}=“${DEFAULT}”")
            unset("${setting_NAME}" CACHE)
        endif()
    endif()

    # Detect the previous value
    unset(prev_val)
    if(DEFINED "CACHE{${setting_NAME}-PREV}")
        set(prev_val "$CACHE{${setting_NAME}-PREV}")
        message(DEBUG "Detected previous value was “${prev_val}”")
    elseif(DEFINED "CACHE{${setting_NAME}}")
        message(DEBUG "Externally defined to be “${${setting_NAME}}”")
    else()
        message(DEBUG "No previous value detected")
    endif()

    # Actually define it now:
    set("${setting_NAME}" "${DEFAULT}" CACHE "${setting_TYPE}" "${doc}")
    # Variable properties:
    set_property(CACHE "${setting_NAME}" PROPERTY HELPSTRING "${doc}")
    set_property(CACHE "${setting_NAME}" PROPERTY TYPE "${setting_TYPE}")
    set_property(CACHE "${setting_NAME}" PROPERTY ADVANCED "${setting_ADVANCED}")
    if(setting_OPTIONS)
        set_property(CACHE "${setting_NAME}" PROPERTY STRINGS "${setting_OPTIONS}")
    endif()

    # Report what we set:
    if(NOT DEFINED prev_val)
        message(VERBOSE "Setting: ${setting_NAME} := “${${setting_NAME}}”")
    elseif("${${setting_NAME}}" STREQUAL prev_val)
        message(DEBUG "Setting: ${setting_NAME} := “${${setting_NAME}}” (Unchanged)")
    else()
        message(VERBOSE "Setting: ${setting_NAME} := “${${setting_NAME}}” (Old value was “${prev_val}”)")
    endif()
    set("${setting_NAME}-PREV" "${${setting_NAME}}" CACHE INTERNAL "Prior value of ${setting_NAME}")

    # Validation of options:
    if((DEFINED setting_OPTIONS) AND (NOT ("${${setting_NAME}}" IN_LIST setting_OPTIONS)))
        message(FATAL_ERROR "The value of “${setting_NAME}” must be one of [${opts}] (Got ${setting_NAME}=“${${setting_NAME}}”)")
    endif()
    string(TOUPPER "${${setting_NAME}}" curval)
    if(setting_TYPE STREQUAL "BOOL"
        AND NOT curval MATCHES "^(1|0|ON|OFF|YES|NO|TRUE|FALSE|Y|N|IGNORE)$")
        message(WARNING "The value of ${setting_NAME}=“${${setting_NAME}}” is not a regular boolean value")
    endif()

    # Custom validation:
    if(DEFINED setting_VALIDATE)
        cmake_parse_arguments(validate "" "CODE" "" ${setting_VALIDATE})
        if(DEFINED validate_CODE)
            _mongo_eval_cmake("" "${validate_CODE}")
        endif()
        if(validate_UNPARSED_ARGUMENTS)
            message(FATAL_ERROR "Unrecognized VALIDATE options: ${validate_UNPARSED_ARGUMENTS}")
        endif()
    endif()

    if(DEFINED setting_VISIBLE_IF)
        string(JOIN "\n" code
            "set(avail FALSE)"
            "if(${setting_VISIBLE_IF})"
            "  set(avail TRUE)"
            "endif()")
        _mongo_eval_cmake(avail "${code}")
        if(NOT avail)
            # Hide the option by making it INTERNAL
            set_property(CACHE "${setting_NAME}" PROPERTY TYPE INTERNAL)
        endif()
    endif()
endfunction()

#[[ Implements DEFAULT setting value logic ]]
function(_mongo_compute_default outvar arglist)
    list(APPEND CMAKE_MESSAGE_CONTEXT default)
    # Clear the value in the caller:
    unset("${outvar}" PARENT_SCOPE)

    # Parse arguments:
    cmake_parse_arguments(dflt "" "" "DEVEL" ${arglist})

    # Developer-mode options:
    if(DEFINED dflt_DEVEL AND MONGODB_DEVELOPER)
        list(APPEND CMAKE_MESSAGE_CONTEXT "devel")
        _mongo_compute_default(tmp "${dflt_DEVEL}")
        message(DEBUG "Detected MONGODB_DEVELOPER: Default of ${setting_NAME} is “${tmp}”")
        set("${outvar}" "${tmp}" PARENT_SCOPE)
        return()
    endif()

    # Parse everything else:
    set(other_args "${dflt_UNPARSED_ARGUMENTS}")
    cmake_parse_arguments(dflt "" "VALUE;EVAL" "" ${other_args})

    if(DEFINED dflt_VALUE)
        # Simple value for the default
        if(DEFINED dflt_EVAL)
            message(FATAL_ERROR "Only one of VALUE or EVAL may be specified for a DEFAULT")
        endif()
        set("${outvar}" "${dflt_VALUE}" PARENT_SCOPE)
    elseif(DEFINED dflt_EVAL)
        # Evaluate some code to determine the default
        _mongo_eval_cmake(DEFAULT "${dflt_EVAL}")
        set("${outvar}" "${DEFAULT}" PARENT_SCOPE)
        if(DEFINED DEFAULT)
            message(DEBUG "Computed default ${setting_NAME} value to be “${DEFAULT}”")
        else()
            message(DEBUG "No default for ${setting_NAME} was computed. Default will be an empty string.")
        endif()
    elseif(dflt_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR
                "${setting_NAME}: "
                "DEFAULT got unexpected arguments: ${dflt_UNPARSED_ARGUMENTS}")
    endif()
endfunction()

#[==[
Define a new boolean build setting::

    mongo_bool_setting(
        <name> <doc>
        [DEFAULT [[DEVEL] [VALUE <value> | EVAL <code>]] ...]
        [VALIDATE [CODE <code>]]
        [ADVANCED]
    )

This is a shorthand for defining a boolean setting. See mongo_setting() for more
option information. The TYPE of the setting will be BOOL, and the implicit
default value for the setting will be ON if no DEFAULT is provided.

]==]
function(mongo_bool_setting name doc)
    set(args ${ARGN})
    # Inject "ON" as a default:
    if(NOT "DEFAULT" IN_LIST args)
        list(APPEND args DEFAULT VALUE ON)
    endif()
    mongo_setting("${name}" "${doc}" TYPE BOOL ${args})
endfunction()

# Set the variable named by 'out' to the 'if_true' or 'if_false' value based on 'cond'
function(mongo_pick out if_true if_false cond)
    string(REPLACE "'" "\"" cond "${cond}")
    mongo_bool01(b "${cond}")
    if(b)
        set("${out}" "${if_true}" PARENT_SCOPE)
    else()
        set("${out}" "${if_false}" PARENT_SCOPE)
    endif()
endfunction()

# Evaluate CMake code <code>, and lift the given variables into the caller's scope.
function(_mongo_eval_cmake get_variables code)
    # Set a name that is unlikely to collide:
    set(__eval_liftvars "${get_variables}")
    # Clear the values before we evaluate the code:
    foreach(__varname IN LISTS __eval_liftvars)
        unset("${__varname}" PARENT_SCOPE)
        unset("${__varname}")
    endforeach()
    # We do the "eval" the old fashion way, since we can't yet use cmake_language()
    message(TRACE "Evaluating CMake code:\n\n${code}")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/_eval.tmp.cmake" "${code}")
    include("${CMAKE_CURRENT_BINARY_DIR}/_eval.tmp.cmake")
    # Lift the variables into the caller's scope
    foreach(__varname IN LISTS __eval_liftvars)
        if(DEFINED "${__varname}")
            message(TRACE "Eval variable result: ${__varname}=${${__varname}}")
            set("${__varname}" "${${__varname}}" PARENT_SCOPE)
        endif()
    endforeach()
endfunction()

#[==[
    mongo_bool01(<var> <cond>)

Evaluate a condition and store the boolean result as a "0" or a "1".

Parameters:

<var>
    - The name of the variable to define in the caller's scope.

<cond>
    - `...cond` The condition to evaluate. It must be a single string that
      contains wraps the syntax of a CMake `if()` command

Example: Evaluate Boolean Logic
###############################

    mongo_bool01(is_mingw [[WIN32 AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU"]])

Note the quoting and use of [[]]-bracket strings

]==]
function(mongo_bool01 var code)
    if(ARGN)
        message(FATAL_ERROR "Too many arguments passed to mongo_bool01")
    endif()
    string(CONCAT fullcode
        "if(${code})\n"
        "  set(bool 1)\n"
        "else()\n"
        "  set(bool 0)\n"
        "endif()\n")
    _mongo_eval_cmake(bool "${fullcode}")
    set("${var}" "${bool}" PARENT_SCOPE)
endfunction()

#[==[
Append usage requirement properties to a set of targets.

    mongo_target_requirements(
        [<target> [...]]
        [INCLUDE_DIRECTORIES [spec...]]
        [LINK_LIBRARIES [spec...]]
        [COMPILE_DEFINITIONS [spec...]]
        [COMPILE_OPTIONS [spec...]]
        [LINK_OPTIONS [spec...]]
        [SOURCES [spec...]]
    )

]==]
function(mongo_target_requirements)
    set(properties
        INCLUDE_DIRECTORIES LINK_LIBRARIES COMPILE_DEFINITIONS
        COMPILE_OPTIONS LINK_OPTIONS SOURCES
    )
    cmake_parse_arguments(PARSE_ARGV 0 ARG "" "" "${properties}")
    foreach(target IN LISTS ARG_UNPARSED_ARGUMENTS)
        if(ARG_INCLUDE_DIRECTORIES)
            target_include_directories("${target}" ${ARG_INCLUDE_DIRECTORIES})
        endif()
        if(ARG_LINK_LIBRARIES)
            target_link_libraries("${target}" ${ARG_LINK_LIBRARIES})
        endif()
        if(ARG_COMPILE_DEFINITIONS)
            target_compile_definitions("${target}" ${ARG_COMPILE_DEFINITIONS})
        endif()
        if(ARG_COMPILE_OPTIONS)
            target_compile_options("${target}" ${ARG_COMPILE_OPTIONS})
        endif()
        if(ARG_LINK_OPTIONS)
            target_link_options("${target}" ${ARG_LINK_OPTIONS})
        endif()
        if(ARG_SOURCES)
            target_sources("${target}" ${ARG_SOURCES})
        endif()
    endforeach()
endfunction()
