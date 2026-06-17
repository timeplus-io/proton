add_definitions(-DPy_BUILD_CORE)
add_definitions(-DPy_BUILD_CORE_BUILTIN)

# FIXME: can we disable load shared library?
add_compile_definitions(HAVE_DLOPEN=0)

set(libpython_target_libs)


##########################################################################
# Modules

set(MODULE_SOURCES # Equivalent to MODULE_OBJS in Makefile.pre
    ${CPYTHON_CMAKE_BINARY_DIR}/CMakeFiles/config.c
    ${CPYTHON_SOURCE_DIR}/Modules/main.c
    ${CPYTHON_SOURCE_DIR}/Modules/gcmodule.c
    ${CPYTHON_SOURCE_DIR}/Modules/getbuildinfo.c
    ${CPYTHON_SOURCE_DIR}/Modules/signalmodule.c
)
# getpath.c is deliberately NOT here: it includes the generated
# frozen_modules/getpath.h, so it is compiled into libpython-static below,
# after the frozen headers exist.  This mirrors Makefile.pre.in, where
# Modules/getpath.o is in LIBRARY_OBJS but not LIBRARY_OBJS_OMIT_FROZEN.

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
        ${CPYTHON_SOURCE_DIR}
)
list(APPEND libpython_target_libs libpython-modules-static)


##########################################################################
# Parser — source list from CPython 3.14 Makefile.pre.in
# POBJS / PEGEN_OBJS / TOKENIZER_OBJS (lexer + tokenizer subdirs added in 3.14)
set(PARSER_SOURCES
    ${CPYTHON_SOURCE_DIR}/Parser/token.c
    ${CPYTHON_SOURCE_DIR}/Parser/myreadline.c
    ${CPYTHON_SOURCE_DIR}/Parser/action_helpers.c
    ${CPYTHON_SOURCE_DIR}/Parser/pegen.c
    ${CPYTHON_SOURCE_DIR}/Parser/pegen_errors.c
    ${CPYTHON_SOURCE_DIR}/Parser/parser.c
    ${CPYTHON_SOURCE_DIR}/Parser/string_parser.c
    ${CPYTHON_SOURCE_DIR}/Parser/peg_api.c
    # Lexer (split out of old tokenize.c in 3.14)
    ${CPYTHON_SOURCE_DIR}/Parser/lexer/buffer.c
    ${CPYTHON_SOURCE_DIR}/Parser/lexer/lexer.c
    ${CPYTHON_SOURCE_DIR}/Parser/lexer/state.c
    # Tokenizer (split out of old tokenize.c in 3.14)
    ${CPYTHON_SOURCE_DIR}/Parser/tokenizer/file_tokenizer.c
    ${CPYTHON_SOURCE_DIR}/Parser/tokenizer/readline_tokenizer.c
    ${CPYTHON_SOURCE_DIR}/Parser/tokenizer/string_tokenizer.c
    ${CPYTHON_SOURCE_DIR}/Parser/tokenizer/utf8_tokenizer.c
    ${CPYTHON_SOURCE_DIR}/Parser/tokenizer/helpers.c
)

add_library(libpython-parser-static STATIC
    ${PARSER_SOURCES}
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
# Objects — source list from CPython 3.14 Makefile.pre.in OBJECT_OBJS
set(OBJECT_SOURCES
    ${CPYTHON_SOURCE_DIR}/Objects/abstract.c
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
    ${CPYTHON_SOURCE_DIR}/Objects/interpolationobject.c
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
    ${CPYTHON_SOURCE_DIR}/Objects/templateobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/tupleobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/typeobject.c
    ${CPYTHON_SOURCE_DIR}/Objects/typevarobject.c
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
if(WITH_MIMALLOC)
    # obmalloc.c does `#include "mimalloc/static.c"`, which in turn does
    # `#include "mimalloc.h"` and `#include "mimalloc/internal.h"`. Those
    # headers live under Include/internal/mimalloc, so that must be on the
    # include path for obmalloc.c to resolve them.
    target_include_directories(libpython-objects-static
        PRIVATE
            ${CPYTHON_HEADER_DIR}/internal/mimalloc
    )
endif()
list(APPEND libpython_target_libs libpython-objects-static)


##########################################################################
# libpython-core — source list from CPython 3.14 Makefile.pre.in PYTHON_OBJS
set(PYTHON_SOURCES
    ${CPYTHON_SOURCE_DIR}/Python/dynload_shlib.c
    ${CPYTHON_SOURCE_DIR}/Python/_warnings.c
    ${CPYTHON_SOURCE_DIR}/Python/Python-ast.c
    ${CPYTHON_SOURCE_DIR}/Python/Python-tokenize.c
    ${CPYTHON_SOURCE_DIR}/Python/asdl.c
    ${CPYTHON_SOURCE_DIR}/Python/ast.c
    ${CPYTHON_SOURCE_DIR}/Python/ast_preprocess.c
    ${CPYTHON_SOURCE_DIR}/Python/ast_unparse.c
    ${CPYTHON_SOURCE_DIR}/Python/assemble.c
    ${CPYTHON_SOURCE_DIR}/Python/bltinmodule.c
    ${CPYTHON_SOURCE_DIR}/Python/brc.c
    ${CPYTHON_SOURCE_DIR}/Python/ceval.c
    ${CPYTHON_SOURCE_DIR}/Python/ceval_gil.c
    ${CPYTHON_SOURCE_DIR}/Python/codecs.c
    ${CPYTHON_SOURCE_DIR}/Python/codegen.c
    ${CPYTHON_SOURCE_DIR}/Python/compile.c
    ${CPYTHON_SOURCE_DIR}/Python/context.c
    ${CPYTHON_SOURCE_DIR}/Python/_contextvars.c
    ${CPYTHON_SOURCE_DIR}/Python/critical_section.c
    ${CPYTHON_SOURCE_DIR}/Python/crossinterp.c
    ${CPYTHON_SOURCE_DIR}/Python/dynamic_annotations.c
    ${CPYTHON_SOURCE_DIR}/Python/errors.c
    ${CPYTHON_SOURCE_DIR}/Python/fileutils.c
    ${CPYTHON_SOURCE_DIR}/Python/flowgraph.c
    ${CPYTHON_SOURCE_DIR}/Python/formatter_unicode.c
    ${CPYTHON_SOURCE_DIR}/Python/frame.c
    ${CPYTHON_SOURCE_DIR}/Python/frozenmain.c
    ${CPYTHON_SOURCE_DIR}/Python/future.c
    ${CPYTHON_SOURCE_DIR}/Python/gc.c
    ${CPYTHON_SOURCE_DIR}/Python/gc_free_threading.c
    ${CPYTHON_SOURCE_DIR}/Python/gc_gil.c
    ${CPYTHON_SOURCE_DIR}/Python/getargs.c
    ${CPYTHON_SOURCE_DIR}/Python/getcompiler.c
    ${CPYTHON_SOURCE_DIR}/Python/getcopyright.c
    ${CPYTHON_SOURCE_DIR}/Python/getplatform.c
    ${CPYTHON_SOURCE_DIR}/Python/getversion.c
    ${CPYTHON_SOURCE_DIR}/Python/hamt.c
    ${CPYTHON_SOURCE_DIR}/Python/hashtable.c
    ${CPYTHON_SOURCE_DIR}/Python/import.c
    ${CPYTHON_SOURCE_DIR}/Python/importdl.c
    ${CPYTHON_SOURCE_DIR}/Python/index_pool.c
    ${CPYTHON_SOURCE_DIR}/Python/initconfig.c
    ${CPYTHON_SOURCE_DIR}/Python/instruction_sequence.c
    ${CPYTHON_SOURCE_DIR}/Python/instrumentation.c
    ${CPYTHON_SOURCE_DIR}/Python/interpconfig.c
    ${CPYTHON_SOURCE_DIR}/Python/intrinsics.c
    ${CPYTHON_SOURCE_DIR}/Python/legacy_tracing.c
    ${CPYTHON_SOURCE_DIR}/Python/lock.c
    ${CPYTHON_SOURCE_DIR}/Python/marshal.c
    ${CPYTHON_SOURCE_DIR}/Python/modsupport.c
    ${CPYTHON_SOURCE_DIR}/Python/mysnprintf.c
    ${CPYTHON_SOURCE_DIR}/Python/mystrtoul.c
    ${CPYTHON_SOURCE_DIR}/Python/object_stack.c
    ${CPYTHON_SOURCE_DIR}/Python/optimizer.c
    ${CPYTHON_SOURCE_DIR}/Python/optimizer_analysis.c
    ${CPYTHON_SOURCE_DIR}/Python/optimizer_symbols.c
    ${CPYTHON_SOURCE_DIR}/Python/parking_lot.c
    ${CPYTHON_SOURCE_DIR}/Python/pathconfig.c
    ${CPYTHON_SOURCE_DIR}/Python/perf_jit_trampoline.c
    ${CPYTHON_SOURCE_DIR}/Python/perf_trampoline.c
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
    ${CPYTHON_SOURCE_DIR}/Python/qsbr.c
    ${CPYTHON_SOURCE_DIR}/Python/remote_debugging.c
    ${CPYTHON_SOURCE_DIR}/Python/bootstrap_hash.c
    ${CPYTHON_SOURCE_DIR}/Python/specialize.c
    ${CPYTHON_SOURCE_DIR}/Python/stackrefs.c
    ${CPYTHON_SOURCE_DIR}/Python/structmember.c
    ${CPYTHON_SOURCE_DIR}/Python/suggestions.c
    ${CPYTHON_SOURCE_DIR}/Python/symtable.c
    ${CPYTHON_SOURCE_DIR}/Python/sysmodule.c
    ${CPYTHON_SOURCE_DIR}/Python/thread.c
    ${CPYTHON_SOURCE_DIR}/Python/traceback.c
    ${CPYTHON_SOURCE_DIR}/Python/tracemalloc.c
    ${CPYTHON_SOURCE_DIR}/Python/uniqueid.c
    ${CPYTHON_SOURCE_DIR}/Python/getopt.c
    ${CPYTHON_SOURCE_DIR}/Python/pystrcmp.c
    ${CPYTHON_SOURCE_DIR}/Python/pystrtod.c
    ${CPYTHON_SOURCE_DIR}/Python/pystrhex.c
    ${CPYTHON_SOURCE_DIR}/Python/dtoa.c
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

# Frozen modules
#
# CPython pre-compiles a fixed set of stdlib modules to marshalled bytecode
# and links them into the interpreter (Python/frozen.c includes one generated
# header per module; Modules/getpath.c includes frozen_modules/getpath.h).
# Upstream's Makefile regenerates those headers at build time with the
# Programs/_freeze_module host tool; we mirror that here so the cpython
# submodule needs no committed generated files.
#
# Mirrors Makefile.pre.in:
#   Programs/_freeze_module: _freeze_module.o getpath_noop.o $(LIBRARY_OBJS_OMIT_FROZEN)
# getpath_noop.c stubs _PyConfig_InitPathConfig to break the chicken-and-egg:
# the real getpath.c needs frozen_modules/getpath.h, which this tool generates.
set(LIBPYTHON_FROZEN_SOURCES)
set(FROZEN_HEADERS_DIR ${CPYTHON_CMAKE_BINARY_DIR}/frozen_headers)

# Quoted includes resolve to the includer's directory before any -I path, so
# pre-generated headers committed into the submodule would silently shadow the
# ones we generate (and ship stale bytecode).  Fail fast instead.
file(GLOB stale_frozen_headers ${CPYTHON_SOURCE_DIR}/Python/frozen_modules/*.h)
if(stale_frozen_headers)
    message(FATAL_ERROR
        "Found committed frozen-module headers in "
        "${CPYTHON_SOURCE_DIR}/Python/frozen_modules/ — these would shadow the "
        "headers generated at build time. The cpython submodule must not "
        "commit Python/frozen_modules/*.h; they are regenerated by the "
        "_freeze_module target.")
endif()

# Build _freeze_module tool for generating frozen module headers
if(IS_PY3)
    if(PY_VERSION VERSION_GREATER_EQUAL "3.11")
        add_executable(_freeze_module
            ${CPYTHON_SOURCE_DIR}/Programs/_freeze_module.c
            ${CPYTHON_SOURCE_DIR}/Modules/getpath_noop.c
        )
        # The component archives and builtin-extension archives have mutual
        # symbol references (config.c -> PyInit_*, extensions -> core);
        # group-rescan so archive order doesn't matter. Apple's ld64 resolves
        # archive cycles on its own and hard-errors on --start-group, so the
        # grouping is GNU-ld only (release_build.yml builds this natively on
        # macOS arm64 with ENABLE_PYTHON_UDF=ON).
        if(APPLE)
            target_link_libraries(_freeze_module PRIVATE
                ${libpython_target_libs}
                ${extension_libraries}
            )
        else()
            target_link_libraries(_freeze_module PRIVATE
                -Wl,--start-group
                ${libpython_target_libs}
                ${extension_libraries}
                -Wl,--end-group
            )
        endif()

        file(MAKE_DIRECTORY ${FROZEN_HEADERS_DIR}/Python/frozen_modules)

        # module-name|source pairs; keep in sync with the
        # Python/frozen_modules/*.h rules in Makefile.pre.in.
        set(FROZEN_MODULES
            # bootstrap (also frozen by the C tool upstream)
            "getpath|Modules/getpath.py"
            "importlib._bootstrap|Lib/importlib/_bootstrap.py"
            "importlib._bootstrap_external|Lib/importlib/_bootstrap_external.py"
            "zipimport|Lib/zipimport.py"
            # stdlib - startup, without site
            "abc|Lib/abc.py"
            "codecs|Lib/codecs.py"
            "io|Lib/io.py"
            # stdlib - startup, with site
            "_collections_abc|Lib/_collections_abc.py"
            "_sitebuiltins|Lib/_sitebuiltins.py"
            "genericpath|Lib/genericpath.py"
            "ntpath|Lib/ntpath.py"
            "posixpath|Lib/posixpath.py"
            "os|Lib/os.py"
            "site|Lib/site.py"
            "stat|Lib/stat.py"
            # runpy - run module with -m
            "importlib.util|Lib/importlib/util.py"
            "importlib.machinery|Lib/importlib/machinery.py"
            "runpy|Lib/runpy.py"
            # test modules referenced by frozen.c's test table
            "__hello__|Lib/__hello__.py"
            "__phello__|Lib/__phello__/__init__.py"
            "__phello__.ham|Lib/__phello__/ham/__init__.py"
            "__phello__.ham.eggs|Lib/__phello__/ham/eggs.py"
            "__phello__.spam|Lib/__phello__/spam.py"
            "frozen_only|Tools/freeze/flag.py"
        )

        foreach(frozen_spec IN LISTS FROZEN_MODULES)
            string(REPLACE "|" ";" frozen_spec_parts "${frozen_spec}")
            list(GET frozen_spec_parts 0 frozen_name)
            list(GET frozen_spec_parts 1 frozen_src)
            set(frozen_header ${FROZEN_HEADERS_DIR}/Python/frozen_modules/${frozen_name}.h)
            add_custom_command(
                OUTPUT ${frozen_header}
                COMMAND _freeze_module ${frozen_name}
                        ${CPYTHON_SOURCE_DIR}/${frozen_src} ${frozen_header}
                DEPENDS _freeze_module ${CPYTHON_SOURCE_DIR}/${frozen_src}
                COMMENT "Freezing module ${frozen_name}"
                VERBATIM
            )
            list(APPEND LIBPYTHON_FROZEN_SOURCES ${frozen_header})
        endforeach()
    else()
        # CPython 3.10 and earlier: use _freeze_importlib
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

        add_custom_target(freeze_modules DEPENDS ${LIBPYTHON_FROZEN_SOURCES})
    endif()
endif()

# ==============================================================================
# libpython
add_library(libpython-static STATIC
    ${CPYTHON_SOURCE_DIR}/Python/frozen.c
    ${CPYTHON_SOURCE_DIR}/Modules/getpath.c
    ${LIBPYTHON_FROZEN_SOURCES}
)
# frozen.c includes "frozen_modules/<mod>.h" (relative to the Python/ dir) and
# getpath.c includes "Python/frozen_modules/getpath.h"; both resolve to the
# headers generated into FROZEN_HEADERS_DIR above.  NOTE: for quoted includes
# the includer's own directory is searched before any -I path, so committed
# headers inside the submodule would silently shadow the generated ones — the
# configure-time guard below rejects that.
target_include_directories(libpython-static
    BEFORE PRIVATE
        ${FROZEN_HEADERS_DIR}/Python
        ${FROZEN_HEADERS_DIR}
        ${CPYTHON_SOURCE_DIR}/Python
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
