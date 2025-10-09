include(CMakeExpandImportedTargets)

macro(python_platform_test var description srcfile invert)
  if(NOT DEFINED "${var}_COMPILED")
    message(STATUS "${description}")

    set(MACRO_CHECK_FUNCTION_DEFINITIONS
      "-D${var} ${CMAKE_REQUIRED_FLAGS}")
    if(CMAKE_REQUIRED_LIBRARIES)
      # this one translates potentially used imported library targets to their files on disk
      CMAKE_EXPAND_IMPORTED_TARGETS(_ADJUSTED_CMAKE_REQUIRED_LIBRARIES  LIBRARIES  ${CMAKE_REQUIRED_LIBRARIES} CONFIGURATION "${CMAKE_TRY_COMPILE_CONFIGURATION}")
      set(CHECK_C_SOURCE_COMPILES_ADD_LIBRARIES
        "-DLINK_LIBRARIES:STRING=${_ADJUSTED_CMAKE_REQUIRED_LIBRARIES}")
    else()
      set(CHECK_C_SOURCE_COMPILES_ADD_LIBRARIES)
    endif()
    if(CMAKE_REQUIRED_INCLUDES)
      set(CHECK_C_SOURCE_COMPILES_ADD_INCLUDES
        "-DINCLUDE_DIRECTORIES:STRING=${CMAKE_REQUIRED_INCLUDES}")
    else()
      set(CHECK_C_SOURCE_COMPILES_ADD_INCLUDES)
    endif()

    try_compile(${var}_COMPILED
      ${CMAKE_CURRENT_BINARY_DIR}
      ${srcfile}
      COMPILE_DEFINITIONS ${CMAKE_REQUIRED_DEFINITIONS}
      CMAKE_FLAGS -DCOMPILE_DEFINITIONS:STRING=${MACRO_CHECK_FUNCTION_DEFINITIONS}
      "${CHECK_C_SOURCE_COMPILES_ADD_LIBRARIES}"
      "${CHECK_C_SOURCE_COMPILES_ADD_INCLUDES}"
      # OUTPUT_VARIABLE OUTPUT # Do NOT use to make cross-compiling easier
      )
    if(${invert} MATCHES INVERT)
      if(${var}_COMPILED)
        message(STATUS "${description} - no")
      else()
        message(STATUS "${description} - yes")
      endif()
    else()
      if(${var}_COMPILED)
        message(STATUS "${description} - yes")
      else()
        message(STATUS "${description} - no")
      endif()
    endif()
  endif()
  if(${invert} MATCHES INVERT)
    if(${var}_COMPILED)
      SET(${var} 0)
    else()
      SET(${var} 1)
    endif()
  else()
    if(${var}_COMPILED)
      SET(${var} 1)
    else()
      SET(${var} 0)
    endif()
  endif()
endmacro()

macro(python_platform_test_run var description srcfile invert)
  set(ORIGINAL_C_FLAGS "${CMAKE_C_FLAGS}")
  set(ORIGINAL_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(ORIGINAL_CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")

  string(REGEX REPLACE "-fsanitize=[^ ]+" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
  string(REGEX REPLACE "-fsanitize=[^ ]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_C_STANDARD_LIBRARIES}")
  if(NOT DEFINED "${var}")
    message(STATUS "${description}")

    set(MACRO_CHECK_FUNCTION_DEFINITIONS
      "-D${var} ${CMAKE_REQUIRED_FLAGS}")
    if(CMAKE_REQUIRED_LIBRARIES)
      # this one translates potentially used imported library targets to their files on disk
      CMAKE_EXPAND_IMPORTED_TARGETS(_ADJUSTED_CMAKE_REQUIRED_LIBRARIES  LIBRARIES  ${CMAKE_REQUIRED_LIBRARIES} CONFIGURATION "${CMAKE_TRY_COMPILE_CONFIGURATION}")
      set(CHECK_C_SOURCE_COMPILES_ADD_LIBRARIES
        "-DLINK_LIBRARIES:STRING=${_ADJUSTED_CMAKE_REQUIRED_LIBRARIES}")
    else()
      set(CHECK_C_SOURCE_COMPILES_ADD_LIBRARIES)
    endif()
    if(CMAKE_REQUIRED_INCLUDES)
      set(CHECK_C_SOURCE_COMPILES_ADD_INCLUDES
        "-DINCLUDE_DIRECTORIES:STRING=${CMAKE_REQUIRED_INCLUDES}")
    else()
      set(CHECK_C_SOURCE_COMPILES_ADD_INCLUDES)
    endif()

    try_run(${var} ${var}_COMPILED
      ${CMAKE_CURRENT_BINARY_DIR}
      ${srcfile}
      COMPILE_DEFINITIONS ${CMAKE_REQUIRED_DEFINITIONS}
      CMAKE_FLAGS -DCOMPILE_DEFINITIONS:STRING=${MACRO_CHECK_FUNCTION_DEFINITIONS}
      -DCMAKE_SKIP_RPATH:BOOL=${CMAKE_SKIP_RPATH}
      "${CHECK_C_SOURCE_COMPILES_ADD_LIBRARIES}"
      "${CHECK_C_SOURCE_COMPILES_ADD_INCLUDES}"
      OUTPUT_VARIABLE OUTPUT # Do NOT use to make cross-compiling easier
      )
    message (STATUS "================${var}_OUTPUT: ${OUTPUT}")
    # Note that ${var} will be a 0 return value on success.
    if(NOT ${var}_COMPILED)
      set(${var} -1 CACHE INTERNAL "${description} failed to compile.")
    endif()

    if(${invert} MATCHES INVERT)
      if(${var}_COMPILED)
        if(${var})
          message(STATUS "${description} - yes")
        else()
          message(STATUS "${description} - no")
        endif()
      else()
        message(STATUS "${description} - failed to compile")
      endif()
    else()
      if(${var}_COMPILED)
        if(${var})
          message(STATUS "${description} - no")
        else()
          message(STATUS "${description} - yes")
        endif()
      else()
        message(STATUS "${description} - failed to compile")
      endif()
    endif()
  endif()

  if(${invert} MATCHES INVERT)
    if(${var}_COMPILED)
      if(${var})
        SET(${var} 1)
      else()
        SET(${var} 0)
      endif()
    else()
      SET(${var} 1)
    endif()
  else()
    if(${var}_COMPILED)
      if(${var})
        SET(${var} 0)
      else()
        SET(${var} 1)
      endif()
    else()
      SET(${var} 0)
    endif()
  endif()

  set(CMAKE_C_FLAGS "${ORIGINAL_C_FLAGS}")
  set(CMAKE_CXX_FLAGS "${ORIGINAL_CXX_FLAGS}")
  set(CMAKE_EXE_LINKER_FLAGS "${ORIGINAL_CMAKE_EXE_LINKER_FLAGS}")
endmacro()

macro(python_check_function name var)
  set(check_src ${PROJECT_BINARY_DIR}/CMakeFiles/ac_fn_c_check_func_${name}.c)
  file(WRITE ${check_src} "
/* Define ${name} to an innocuous variant, in case <limits.h> declares ${name}.
   For example, HP-UX 11i <limits.h> declares gettimeofday.  */
#define ${name} innocuous_${name}

/* System header to define __stub macros and hopefully few prototypes,
    which can conflict with char ${name} (); below.
    Prefer <limits.h> to <assert.h> if __STDC__ is defined, since
    <limits.h> exists even on freestanding compilers.  */

#ifdef __STDC__
# include <limits.h>
#else
# include <assert.h>
#endif

#undef ${name}

/* Override any GCC internal prototype to avoid an error.
   Use char because int might match the return type of a GCC
   builtin and then its argument prototype would still apply.  */
#ifdef __cplusplus
extern \"C\"
#endif
char ${name} ();
/* The GNU C library defines this for functions which it implements
    to always fail with ENOSYS.  Some functions are actually named
    something starting with __ and the normal name is an alias.  */
#if defined __stub_${name} || defined __stub___${name}
choke me
#endif

int main () { return ${name} (); }
")

  python_platform_test(
    ${var}
    "Checking for ${name}"
    ${check_src}
    DIRECT
    )
endmacro()
