add_definitions(-DPy_BUILD_CORE)
add_definitions(-DPy_BUILD_CORE_BUILTIN)

# FIXME: can we disable load shared library?
add_compile_definitions(HAVE_DLOPEN=0)

set(libpython_target_libs)


##########################################################################
# Modules

set(MODULE_SOURCES # Equivalent to MODULE_OBJS in Makefile.pre
    ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/config.c
    ${CPYTHON_SOURCE_DIR}/Modules/getpath.c
    ${CPYTHON_SOURCE_DIR}/Modules/main.c
    ${CPYTHON_SOURCE_DIR}/Modules/gcmodule.c
    ${CPYTHON_SOURCE_DIR}/Modules/getbuildinfo.c
    ${CPYTHON_SOURCE_DIR}/Modules/signalmodule.c
)

# Special rules for object files (getpath.o) in Makefile.pre
# set(PYTHONPATH "${EXTRA_PYTHONPATH}:lib-dynload:plat-${PY_PLATFORM}")

set_property(
    SOURCE ${CPYTHON_SOURCE_DIR}/Modules/getpath.c
    PROPERTY COMPILE_DEFINITIONS
        PREFIX="${CMAKE_INSTALL_PREFIX}"
        EXEC_PREFIX="${CMAKE_INSTALL_PREFIX}"
        VERSION="${PY_VERSION_MAJOR}.${PY_VERSION_MINOR}"
        VPATH="."
        PYTHONPATH="${PYTHONPATH}"
)

add_library(libpython-modules-static STATIC
    ${MODULE_SOURCES}
)
target_compile_definitions(libpython-modules-static
    PUBLIC
        Py_NO_ENABLE_SHARED
)
target_include_directories(libpython-modules-static
    PUBLIC
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
)
list(APPEND libpython_target_libs libpython-modules-static)


##########################################################################
# Parser
set(PARSER_COMMON_SOURCES # Equivalent to POBJS in Makefile.pre
    ${CPYTHON_SOURCE_DIR}/Parser/token.c
    ${CPYTHON_SOURCE_DIR}/Parser/myreadline.c
    ${CPYTHON_SOURCE_DIR}/Parser/tokenizer.c
)

set(PARSER_PEGEN_SOURCES
    ${CPYTHON_SOURCE_DIR}/Parser/pegen.c
    ${CPYTHON_SOURCE_DIR}/Parser/parser.c
    ${CPYTHON_SOURCE_DIR}/Parser/string_parser.c
    ${CPYTHON_SOURCE_DIR}/Parser/peg_api.c
)

# PARSER_OBJS in Makefile.pre
add_library(libpython-parser-static STATIC
    ${PARSER_COMMON_SOURCES}
    ${PARSER_PEGEN_SOURCES}
)
target_compile_definitions(libpython-parser-static
    PUBLIC
        Py_NO_ENABLE_SHARED
)
target_include_directories(libpython-parser-static
    PUBLIC
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
)
list(APPEND libpython_target_libs libpython-parser-static)


##########################################################################
# Objects
set(OBJECT_SOURCES # Equivalent to OBJECT_OBJS in Makefile.pre
    ${CPYTHON_SOURCE_DIR}/Objects/abstract.c
    ${CPYTHON_SOURCE_DIR}/Objects/accu.c
    ${CPYTHON_SOURCE_DIR}/Objects/boolobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/bytes_methods.c
    ${CPYTHON_SOURCE_DIR}/Objects/bytearrayobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/bytesobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/call.c
    ${CPYTHON_SOURCE_DIR}/Objects/capsule.c
    ${CPYTHON_SOURCE_DIR}/Objects/cellobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/classobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/codeobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/complexobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/descrobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/enumobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/exceptions.c
    ${CPYTHON_SOURCE_DIR}/Objects/genericaliasobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/genobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/fileobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/floatobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/frameobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/funcobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/interpreteridobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/iterobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/listobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/longobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/dictobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/odictobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/memoryobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/methodobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/moduleobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/namespaceobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/object.c
    ${CPYTHON_SOURCE_DIR}/Objects/obmalloc.c
    ${CPYTHON_SOURCE_DIR}/Objects/picklebufobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/rangeobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/setobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/sliceobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/structseq.c
    ${CPYTHON_SOURCE_DIR}/Objects/tupleobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/typeobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/unicodeobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/unicodectype.c
    ${CPYTHON_SOURCE_DIR}/Objects/unionobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/weakrefobject.c
)

if(CMAKE_C_COMPILER_ID MATCHES GNU)
    foreach(filename class complex float int method string type unicode weakref)
        set_property(SOURCE ${CPYTHON_SOURCE_DIR}/Objects/${filename}object.c PROPERTY COMPILE_FLAGS -Wno-unused-value)
    endforeach()
endif()

add_library(libpython-objects-static STATIC
    ${OBJECT_SOURCES}
)
target_compile_definitions(libpython-objects-static
    PUBLIC
        Py_NO_ENABLE_SHARED
)
target_include_directories(libpython-objects-static
    PUBLIC
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
)
list(APPEND libpython_target_libs libpython-objects-static)


##########################################################################
# libpython-core
set(PYTHON_SOURCES
    ${CPYTHON_SOURCE_DIR}/Python/dynload_shlib.c
    ${CPYTHON_SOURCE_DIR}/Python/_warnings.c
    ${CPYTHON_SOURCE_DIR}/Python/Python-ast.c
    ${CPYTHON_SOURCE_DIR}/Python/asdl.c
    ${CPYTHON_SOURCE_DIR}/Python/ast.c
    ${CPYTHON_SOURCE_DIR}/Python/ast_opt.c
    ${CPYTHON_SOURCE_DIR}/Python/ast_unparse.c
    ${CPYTHON_SOURCE_DIR}/Python/bltinmodule.c
    ${CPYTHON_SOURCE_DIR}/Python/ceval.c
    ${CPYTHON_SOURCE_DIR}/Python/codecs.c
    ${CPYTHON_SOURCE_DIR}/Python/compile.c
    ${CPYTHON_SOURCE_DIR}/Python/context.c
    ${CPYTHON_SOURCE_DIR}/Python/dynamic_annotations.c
    ${CPYTHON_SOURCE_DIR}/Python/errors.c
    ${CPYTHON_SOURCE_DIR}/Python/frozenmain.c
    ${CPYTHON_SOURCE_DIR}/Python/future.c
    ${CPYTHON_SOURCE_DIR}/Python/getargs.c
    ${CPYTHON_SOURCE_DIR}/Python/getcompiler.c
    ${CPYTHON_SOURCE_DIR}/Python/getcopyright.c
    ${CPYTHON_SOURCE_DIR}/Python/getplatform.c
    ${CPYTHON_SOURCE_DIR}/Python/getversion.c
    ${CPYTHON_SOURCE_DIR}/Python/hamt.c
    ${CPYTHON_SOURCE_DIR}/Python/hashtable.c
    ${CPYTHON_SOURCE_DIR}/Python/import.c
    ${CPYTHON_SOURCE_DIR}/Python/importdl.c
    ${CPYTHON_SOURCE_DIR}/Python/initconfig.c
    ${CPYTHON_SOURCE_DIR}/Python/marshal.c
    ${CPYTHON_SOURCE_DIR}/Python/modsupport.c
    ${CPYTHON_SOURCE_DIR}/Python/mysnprintf.c
    ${CPYTHON_SOURCE_DIR}/Python/mystrtoul.c
    ${CPYTHON_SOURCE_DIR}/Python/pathconfig.c
    ${CPYTHON_SOURCE_DIR}/Python/preconfig.c
    ${CPYTHON_SOURCE_DIR}/Python/pyarena.c
    ${CPYTHON_SOURCE_DIR}/Python/pyctype.c
    ${CPYTHON_SOURCE_DIR}/Python/pyfpe.c
    ${CPYTHON_SOURCE_DIR}/Python/pyhash.c
    ${CPYTHON_SOURCE_DIR}/Python/pylifecycle.c
    ${CPYTHON_SOURCE_DIR}/Python/pymath.c
    ${CPYTHON_SOURCE_DIR}/Python/pystate.c
    ${CPYTHON_SOURCE_DIR}/Python/pythonrun.c
    ${CPYTHON_SOURCE_DIR}/Python/pytime.c
    ${CPYTHON_SOURCE_DIR}/Python/bootstrap_hash.c
    ${CPYTHON_SOURCE_DIR}/Python/structmember.c
    ${CPYTHON_SOURCE_DIR}/Python/symtable.c
    ${CPYTHON_SOURCE_DIR}/Python/sysmodule.c
    ${CPYTHON_SOURCE_DIR}/Python/thread.c
    ${CPYTHON_SOURCE_DIR}/Python/traceback.c
    ${CPYTHON_SOURCE_DIR}/Python/getopt.c
    ${CPYTHON_SOURCE_DIR}/Python/pystrcmp.c
    ${CPYTHON_SOURCE_DIR}/Python/pystrtod.c
    ${CPYTHON_SOURCE_DIR}/Python/pystrhex.c
    ${CPYTHON_SOURCE_DIR}/Python/dtoa.c
    ${CPYTHON_SOURCE_DIR}/Python/formatter_unicode.c
    ${CPYTHON_SOURCE_DIR}/Python/fileutils.c
    ${CPYTHON_SOURCE_DIR}/Python/suggestions.c
    # ${CPYTHON_SOURCE_DIR}/$(LIBOBJS)
    # ${CPYTHON_SOURCE_DIR}/$(MACHDEP_OBJS)
    # ${CPYTHON_SOURCE_DIR}/$(DTRACE_OBJS)
)

set_property(
    SOURCE ${CPYTHON_SOURCE_DIR}/Python/getplatform.c
    PROPERTY COMPILE_DEFINITIONS
        PLATFORM="${PY_PLATFORM}"
)

set_property(
    SOURCE ${CPYTHON_SOURCE_DIR}/Python/sysmodule.c
    PROPERTY COMPILE_DEFINITIONS
        ABIFLAGS="${ABIFLAGS}"
        MULTIARCH="${PLATFORM_TRIPLET}"
)

set_property(
    SOURCE ${CPYTHON_SOURCE_DIR}/Python/dynload_shlib.c
    PROPERTY COMPILE_DEFINITIONS
        SOABI="${SOABI}"
)

add_library(libpython-core-static STATIC
    ${PYTHON_SOURCES}
)
target_compile_definitions(libpython-core-static
    PUBLIC
        Py_NO_ENABLE_SHARED
)
target_compile_options(libpython-core-static PRIVATE -fPIC)
target_include_directories(libpython-core-static
    PUBLIC
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
)
list(APPEND libpython_target_libs libpython-core-static)


# Prepare variables for generating config.c
get_property(config_inits GLOBAL PROPERTY config_inits)
get_property(config_entries GLOBAL PROPERTY config_entries)

if(PY_VERSION VERSION_GREATER_EQUAL "3.7")
    set(PyInit_imp "PyInit__imp")
else()
    set(PyInit_imp "PyInit_imp")
endif()

configure_file(
    ${CPYTHON_CMAKE_SOURCE_DIR}/cmake/config_${PY_VERSION_MAJOR}.c.in
    ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/config.c
    )

get_property(extension_libraries GLOBAL PROPERTY extension_libraries)
# ==============================================================================

set(LIBPYTHON_FROZEN_SOURCES )
if(IS_PY3)

# Build _freeze_importlib executable
add_executable(_freeze_importlib
    ${CPYTHON_SOURCE_DIR}/Programs/_freeze_importlib.c
)
target_link_libraries(_freeze_importlib
    ${libpython_target_libs}
    ${extension_libraries}
)

if(builtin_compile_definitions_without_py_limited_api)
  target_compile_definitions(_freeze_importlib PUBLIC ${builtin_compile_definitions_without_py_limited_api})
endif()

target_include_directories(_freeze_importlib
    PUBLIC
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
)

# Freeze modules
set(LIBPYTHON_FROZEN_SOURCES
  ${CPYTHON_SOURCE_DIR}/Python/importlib_external.h
  ${CPYTHON_SOURCE_DIR}/Python/importlib.h
)
if(PY_VERSION VERSION_GREATER_EQUAL "3.8")
  list(APPEND LIBPYTHON_FROZEN_SOURCES
    ${CPYTHON_SOURCE_DIR}/Python/importlib_zipimport.h
  )
endif()
add_custom_command(
  OUTPUT ${LIBPYTHON_FROZEN_SOURCES}
  COMMAND
    ${CMAKE_CROSSCOMPILING_EMULATOR} $<TARGET_FILE:_freeze_importlib>
      $<$<VERSION_GREATER_EQUAL:${PY_VERSION},3.8>:importlib._bootstrap_external>
      ${CPYTHON_SOURCE_DIR}/Lib/importlib/_bootstrap_external.py
      ${CPYTHON_SOURCE_DIR}/Python/importlib_external.h
  COMMAND
    ${CMAKE_CROSSCOMPILING_EMULATOR} $<TARGET_FILE:_freeze_importlib>
      $<$<VERSION_GREATER_EQUAL:${PY_VERSION},3.8>:importlib._bootstrap>
      ${CPYTHON_SOURCE_DIR}/Lib/importlib/_bootstrap.py
      ${CPYTHON_SOURCE_DIR}/Python/importlib.h
  DEPENDS
    _freeze_importlib
    ${CPYTHON_SOURCE_DIR}/Lib/importlib/_bootstrap_external.py
    ${CPYTHON_SOURCE_DIR}/Lib/importlib/_bootstrap.py
)
if(PY_VERSION VERSION_GREATER_EQUAL "3.8")
  add_custom_command(
    OUTPUT ${LIBPYTHON_FROZEN_SOURCES}
    COMMAND
      ${CMAKE_CROSSCOMPILING_EMULATOR} $<TARGET_FILE:_freeze_importlib>
        zipimport
        ${CPYTHON_SOURCE_DIR}/Lib/zipimport.py
        ${CPYTHON_SOURCE_DIR}/Python/importlib_zipimport.h
    DEPENDS
      ${CPYTHON_SOURCE_DIR}/Lib/zipimport.py
    APPEND
  )
endif()

# This is a convenience target allowing to regenerate
# the frozen sources.
add_custom_target(freeze_modules DEPENDS ${LIBPYTHON_FROZEN_SOURCES})

endif()

# ==============================================================================
# libpython
add_library(libpython-static STATIC
    ${CPYTHON_SOURCE_DIR}/Python/frozen.c
    ${LIBPYTHON_FROZEN_SOURCES}
)
target_compile_definitions(libpython-static
    PUBLIC
        Py_NO_ENABLE_SHARED
)
target_link_libraries(libpython-static
    PUBLIC
        ${libpython_target_libs}
        ${extension_libraries}
)
target_include_directories(libpython-static
    SYSTEM BEFORE INTERFACE
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_HEADER_DIR}/internal
        ${CPYTHON_CMAKE_BINARY_DIR}/include
)
