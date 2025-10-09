include (CheckCSourceCompiles)
include (CMakePushCheckState)
include (MongoSettings)

mongo_setting (
   MONGO_SANITIZE "Semicolon/comma-separated list of sanitizers to apply when building"
   DEFAULT
      DEVEL EVAL [[
         if(NOT MSVC)
            set(DEFAULT "address,undefined")
         endif()
      ]])

# Replace commas with semicolons for the genex
string(REPLACE ";" "," _sanitize "${MONGO_SANITIZE}")

if (_sanitize)
    string (MAKE_C_IDENTIFIER "HAVE_SANITIZE_${_sanitize}" ident)
    string (TOUPPER "${ident}" varname)
    set (flag "-fsanitize=${_sanitize}")

    cmake_push_check_state ()
        set (CMAKE_REQUIRED_FLAGS "${flag}")
        set (CMAKE_REQUIRED_LIBRARIES "${flag}")
        check_c_source_compiles ([[
            #include <stdio.h>

            int main (void) {
                puts ("Hello, world!");
                return 0;
            }
        ]] "${varname}")
    cmake_pop_check_state ()

    if (NOT "${${varname}}")
        message (SEND_ERROR "Requested sanitizer option '${flag}' is not supported by the compiler+linker")
    else ()
        message (STATUS "Enabling sanitizers: ${flag}")
        mongo_platform_compile_options ($<BUILD_INTERFACE:${flag}>)
        mongo_platform_link_options (${flag})
    endif ()
endif ()
