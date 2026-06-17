
add_python_extension(array SOURCES arraymodule.c)
# audioop was removed in CPython 3.13
add_python_extension(_bisect SOURCES _bisectmodule.c)
add_python_extension(cmath SOURCES cmathmodule.c LIBRARIES ${M_LIBRARIES})
add_python_extension(_codecs_cn SOURCES cjkcodecs/_codecs_cn.c)
add_python_extension(_codecs_hk SOURCES cjkcodecs/_codecs_hk.c)
add_python_extension(_codecs_iso2022 SOURCES cjkcodecs/_codecs_iso2022.c)
add_python_extension(_codecs_jp SOURCES cjkcodecs/_codecs_jp.c)
add_python_extension(_codecs_kr SOURCES cjkcodecs/_codecs_kr.c)
add_python_extension(_codecs_tw SOURCES cjkcodecs/_codecs_tw.c)
add_python_extension(_collections SOURCES _collectionsmodule.c) # Container types

# set(crypt2_NAME crypt)
# set(crypt2_SOURCES cryptmodule.c)
# set(crypt3_NAME _crypt)
# set(crypt3_SOURCES _cryptmodule.c)
# add_python_extension(${crypt${PY_VERSION_MAJOR}_NAME} REQUIRES HAVE_LIBCRYPT SOURCES ${crypt${PY_VERSION_MAJOR}_SOURCES} LIBRARIES ${HAVE_LIBCRYPT})

add_python_extension(_csv SOURCES _csv.c)
add_python_extension(_ctypes_test SOURCES _ctypes/_ctypes_test.c LIBRARIES ${M_LIBRARIES})

set(datetime2_NAME datetime)
set(datetime2_SOURCES datetimemodule.c)
set(datetime3_NAME _datetime)
set(datetime3_SOURCES _datetimemodule.c)
if(PY_VERSION VERSION_EQUAL "3.2")
  list(APPEND datetime${PY_VERSION_MAJOR}_SOURCES _time.c)
elseif(PY_VERSION VERSION_LESS_EQUAL "3.1")
  list(APPEND datetime${PY_VERSION_MAJOR}_SOURCES timemodule.c)
endif()
add_python_extension(${datetime${PY_VERSION_MAJOR}_NAME} SOURCES ${datetime${PY_VERSION_MAJOR}_SOURCES} LIBRARIES ${M_LIBRARIES})
if(CMAKE_C_COMPILER_ID MATCHES GNU)
   set_property(SOURCE ${CPYTHON_SOURCE_DIR}/Modules/datetimemodule.c PROPERTY COMPILE_FLAGS -Wno-unused-value)
endif()

add_python_extension(_functools SOURCES _functoolsmodule.c DEFINITIONS Py_BUILD_CORE) # Tools for working with functions and callable objects

add_python_extension(_heapq SOURCES _heapqmodule.c)

set(_io_SOURCES
    _io/_iomodule.c
    _io/bufferedio.c
    _io/bytesio.c
    _io/fileio.c
    _io/iobase.c
    _io/stringio.c
    _io/textio.c
    )
add_python_extension(_io SOURCES ${_io_SOURCES} DEFINITIONS Py_BUILD_CORE)

add_python_extension(itertools SOURCES itertoolsmodule.c) # Functions creating iterators for efficient looping
add_python_extension(_json SOURCES _json.c)
add_python_extension(_locale SOURCES _localemodule.c) # access to ISO C locale support
add_python_extension(_lsprof SOURCES _lsprof.c rotatingtree.c)
add_python_extension(math SOURCES mathmodule.c LIBRARIES ${M_LIBRARIES})
add_python_extension(mmap SOURCES mmapmodule.c)
add_python_extension(_multibytecodec SOURCES cjkcodecs/multibytecodec.c)

# FIXME: Cannot find source file
# add_python_extension(parser SOURCES parsermodule.c)

add_python_extension(_random SOURCES _randommodule.c)

add_python_extension(_struct SOURCES _struct.c)
# _testcapi removed: CPython test-only module, not needed for embedding

set(thread3_NAME _thread)
set(thread3_SOURCES ${CPYTHON_SOURCE_DIR}/Modules/_threadmodule.c)

add_python_extension(${thread3_NAME} SOURCES ${thread3_SOURCES})

set(time_SOURCES timemodule.c)
if(PY_VERSION VERSION_EQUAL "3.2")
  list(APPEND datetime${PY_VERSION_MAJOR}_SOURCES _time.c)
endif()
add_python_extension(time SOURCES ${time_SOURCES} DEFINITIONS Py_BUILD_CORE)

add_python_extension(unicodedata SOURCES unicodedata.c)

# Python3
add_python_extension(atexit SOURCES atexitmodule.c) # Register functions to be run at interpreter-shutdown
add_python_extension(_codecs SOURCES _codecsmodule.c) # access to the builtin codecs and codec registry
add_python_extension(faulthandler SOURCES faulthandler.c)
add_python_extension(_opcode SOURCES _opcode.c)
add_python_extension(_operator SOURCES _operator.c)
add_python_extension(_pickle SOURCES _pickle.c)
add_python_extension(_sre SOURCES _sre/sre.c) # Fredrik Lundh's new regular expressions
add_python_extension(_stat SOURCES _stat.c) # stat.h interface
add_python_extension(_symtable SOURCES symtablemodule.c)
# Python PEP-3118 (buffer protocol) test module
add_python_extension(_testbuffer SOURCES _testbuffer.c)

# Test loading multiple modules from one compiled file (http://bugs.python.org/issue16421)
add_python_extension(_testimportmultiple SOURCES _testimportmultiple.c)

# Test multi-phase extension module init (PEP 489)
add_python_extension(_testmultiphase SOURCES _testmultiphase.c)

# debug tool to trace memory blocks allocated by Python
add_python_extension(_tracemalloc
    SOURCES ${CPYTHON_SOURCE_DIR}/$<IF:$<VERSION_GREATER_EQUAL:${PY_VERSION},3.9>,Python,Modules>/hashtable.c _tracemalloc.c
)

add_python_extension(_weakref SOURCES _weakref.c)

math(EXPR _limited_api_version "${PY_VERSION_MAJOR} * 100 + ${PY_VERSION_MINOR}")
# Free-threaded builds don't support the Limited API, so skip the xxlimited
# demo module. CPython itself drops it from the Modules/Setup.stdlib list
# for --disable-gil builds.
if(NOT Py_GIL_DISABLED)
    add_python_extension(xxlimited
        SOURCES xxlimited.c
        DEFINITIONS Py_LIMITED_API=0x${_limited_api_version}0000
    )
endif()

add_python_extension(xxsubtype SOURCES xxsubtype.c)

if(PY_VERSION VERSION_LESS "3.8")
    # The zipimport module is always imported at startup. Having it as a
    # builtin module avoids some bootstrapping problems and reduces overhead.
    add_python_extension(zipimport SOURCES zipimport.c DEFINITIONS Py_BUILD_CORE)
endif()

# Python 3.6
if(PY_VERSION VERSION_GREATER_EQUAL "3.6")
    # asyncio speedups
    add_python_extension(_asyncio SOURCES _asynciomodule.c)
    # HACL* cryptographic library used by hash modules in CPython 3.14+
    set(_HACL_DIR ${CPYTHON_SOURCE_DIR}/Modules/_hacl)
    set(_HACL_INCLUDE ${_HACL_DIR}/include)
    add_library(libpython_hacl STATIC
        ${_HACL_DIR}/Hacl_Hash_MD5.c
        ${_HACL_DIR}/Hacl_Hash_SHA1.c
        ${_HACL_DIR}/Hacl_Hash_SHA2.c
        ${_HACL_DIR}/Hacl_Hash_SHA3.c
        ${_HACL_DIR}/Hacl_Hash_Blake2s.c
        ${_HACL_DIR}/Hacl_Hash_Blake2b.c
        ${_HACL_DIR}/Lib_Memzero0.c
    )
    target_include_directories(libpython_hacl PUBLIC
        ${_HACL_DIR}
        ${_HACL_INCLUDE}
        ${_HACL_DIR}/internal
        ${CPYTHON_HEADER_DIR}
        ${CPYTHON_CMAKE_BINARY_DIR}/include
    )
    target_compile_definitions(libpython_hacl PUBLIC Py_NO_ENABLE_SHARED)

    add_python_extension(_blake2 SOURCES blake2module.c INCLUDEDIRS ${_HACL_INCLUDE} ${_HACL_DIR} LIBRARIES libpython_hacl)
    add_python_extension(_sha3 SOURCES sha3module.c INCLUDEDIRS ${_HACL_INCLUDE} ${_HACL_DIR} LIBRARIES libpython_hacl)
endif()

# Python 3.7
if(PY_VERSION VERSION_GREATER_EQUAL "3.7")
    add_python_extension(_abc SOURCES _abc.c)
    # _contextvars was moved into the core interpreter in 3.14 (Python/_contextvars.c)
    add_python_extension(_queue SOURCES _queuemodule.c)

    # FIXME: UUID_LIBRARY
    # add_python_extension(_uuid REQUIRES UUID_LIBRARY "HAVE_UUID_H OR HAVE_UUID_UUID_H" SOURCES _uuidmodule.c LIBRARIES ${UUID_LIBRARY})

    # FIXME: it causes leak sanitizer not work. contrib/cpython/Modules/_xxtestfuzz/fuzzer.c:410 `int __lsan_is_turned_off(void) { return 1; }`
    #       it turns off leak sanitizer (contrib/llvm-project/compiler-rt/lib/lsan/lsan_common.cpp:711 `static bool CheckForLeaks() { if (&__lsan_is_turned_off && __lsan_is_turned_off())`)
    # add_python_extension(_xxtestfuzz SOURCES _xxtestfuzz/_xxtestfuzz.c
    #                                          _xxtestfuzz/fuzzer.c
    # )
endif()

# FIXME: DDLNAME?
# Python 3.8
# if(PY_VERSION VERSION_GREATER_EQUAL "3.8")
#     add_python_extension(_testinternalcapi SOURCES _testinternalcapi.c DEFINITIONS "PY3_DLLNAME=\"python3$<$<CONFIG:Debug>:_d>\"")
# endif()

# FIXME: Cannot find source file
# Python 3.9
# if(PY_VERSION VERSION_GREATER_EQUAL "3.9")
#     add_python_extension(_peg_parser SOURCES _peg_parser.c)
# endif()

# Python 3.14 bootstrap modules
if(PY_VERSION VERSION_GREATER_EQUAL "3.14")
    add_python_extension(_typing SOURCES _typingmodule.c)
    add_python_extension(_types SOURCES _typesmodule.c)
    add_python_extension(_sysconfig SOURCES _sysconfig.c)
    add_python_extension(_suggestions SOURCES _suggestions.c)
endif()

# UNIX-only extensions
add_python_extension(fcntl SOURCES fcntlmodule.c)
add_python_extension(grp SOURCES grpmodule.c)

# FIXME: libtirpc
# set(nis_REQUIRES UNIX HAVE_LIBNSL)
# set(nis_LIBRARIES ${HAVE_LIBNSL})
# set(nis_INCLUDEDIRS )
# if(TIRPC_LIBRARY AND TIRPC_RPC_INCLUDE_PATH)
#     # if rpc.h is provided by libtirpc (instead of being provided by glibc). See python/cpython#5137
#     list(APPEND nis_LIBRARIES ${TIRPC_LIBRARY})
#     list(APPEND nis_INCLUDEDIRS ${TIRPC_RPC_INCLUDE_PATH}/../)
#     if(WITH_STATIC_DEPENDENCIES AND TIRPC_LIBRARY_STATIC_DEPENDENCY_REQUIRED)
#         list(APPEND nis_REQUIRES TIRPC_STATIC_LIBRARIES)
#         list(APPEND nis_LIBRARIES ${TIRPC_STATIC_LIBRARIES})
#     endif()
# endif()
# add_python_extension(nis
#     REQUIRES ${nis_REQUIRES}
#     SOURCES nismodule.c
#     LIBRARIES ${nis_LIBRARIES}
#     INCLUDEDIRS ${nis_INCLUDEDIRS}
# )

add_python_extension(posix SOURCES posixmodule.c DEFINITIONS Py_BUILD_CORE)

add_python_extension(pwd SOURCES pwdmodule.c) # this is needed to find out the user's home dir if $HOME is not set
add_python_extension(resource SOURCES resource.c)

# FIXME: HAVE_GETSPNAM
# add_python_extension(spwd REQUIRES UNIX HAVE_GETSPNAM HAVE_GETSPENT SOURCES spwdmodule.c)

add_python_extension(syslog SOURCES syslogmodule.c)
add_python_extension(termios SOURCES termios.c)

# Python3: UNIX-only extensions
add_python_extension(errno SOURCES errnomodule.c)
add_python_extension(_posixsubprocess SOURCES _posixsubprocess.c)

# FIXME: HAVE_LIBCOREFOUNDATION, HAVE_LIBSYSTEMCONFIGURATION
# # MacOSX-only extensions
if (APPLE AND HAVE_LIBSYSTEMCONFIGURATION AND HAVE_LIBCOREFOUNDATION)
    set(_scproxy3_SOURCES ${CPYTHON_SOURCE_DIR}/Modules/_scproxy.c)
    add_python_extension(_scproxy
        SOURCES ${_scproxy${PY_VERSION_MAJOR}_SOURCES}
        LIBRARIES "-framework SystemConfiguration -framework CoreFoundation"
    )
endif()

# FIXME: fatal error: 'sys/soundcard.h' file not found
# if (LINUX)
#     add_python_extension(ossaudiodev SOURCES ossaudiodev.c)
# endif()

# FIXME: rt
# # Python 3.8: UNIX-only extensions
# if(PY_VERSION VERSION_GREATER_EQUAL "3.8")
#     set(_posixshmem_LIBRARIES)
#     if(HAVE_SHM_OPEN_IN_RT)
#         list(APPEND _posixshmem_LIBRARIES rt)
#     endif()
#     add_python_extension(_posixshmem REQUIRES UNIX SOURCES _multiprocessing/posixshmem.c LIBRARIES ${_posixshmem_LIBRARIES})
# endif()

# Multiprocessing is different on unix and windows
set(_multiprocessing2_SOURCES
    _multiprocessing/socket_connection.c
)
set(_multiprocessing3_SOURCES )
if(HAVE_SEM_OPEN AND NOT POSIX_SEMAPHORES_NOT_ENABLED)
    list(APPEND _multiprocessing${PY_VERSION_MAJOR}_SOURCES
        _multiprocessing/semaphore.c
    )
endif()
set(_multiprocessing_REQUIRES)
if(PY_VERSION VERSION_LESS "3.7")
    list(APPEND _multiprocessing_REQUIRES WITH_THREAD)
endif()
add_python_extension(_multiprocessing
    SOURCES _multiprocessing/multiprocessing.c
            ${_multiprocessing${PY_VERSION_MAJOR}_SOURCES}
)

add_python_extension(select SOURCES selectmodule.c)

set(_socket_SOURCES socketmodule.c)
add_python_extension(_socket
    SOURCES ${_socket_SOURCES}
)

# Common ctypes sources
set(ctypes_COMMON_SOURCES
    _ctypes/_ctypes.c
    _ctypes/callbacks.c
    _ctypes/callproc.c
    _ctypes/stgdict.c
    _ctypes/cfield.c
)

add_python_extension(_ctypes
    SOURCES ${ctypes_COMMON_SOURCES}
    DEFINITIONS Py_BUILD_CORE_MODULE
    LIBRARIES ch_contrib::libffi
)

# # Python3: _decimal
# if(IS_PY3)
# FIXME: LIBMPDEC_LIBRARIES
# set(libmpdec_config_x64          CONFIG_64 ASM)
# set(libmpdec_config_uint128      CONFIG_64 ANSI HAVE_UINT128_T)
# set(libmpdec_config_ansi64       CONFIG_64 ANSI)
# set(libmpdec_config_ppro         CONFIG_32 PPRO ASM)
# set(libmpdec_config_ansi32       CONFIG_32 PPRO) # XXX Is ANSI needed here ?
# set(libmpdec_config_ansi_legacy  CONFIG_32 ANSI LEGACY_COMPILER)
# set(libmpdec_config_universal    UNIVERSAL)

# if(USE_SYSTEM_LIBMPDEC)
#     set(_decimal_REQUIRES REQUIRES LIBMPDEC_LIBRARIES)
#     set(_decimal_LIBRARIES LIBRARIES ${LIBMPDEC_LIBRARIES})
#     set(_decimal_EXTRA_SOURCES )
#     set(_decimal_INCLUDEDIRS )
# else()
#     set(_decimal_REQUIRES )
#     set(_decimal_EXTRA_SOURCES
#         _decimal/libmpdec/basearith.c
#         _decimal/libmpdec/constants.c
#         _decimal/libmpdec/context.c
#         _decimal/libmpdec/convolute.c
#         _decimal/libmpdec/crt.c
#         _decimal/libmpdec/difradix2.c
#         _decimal/libmpdec/fnt.c
#         _decimal/libmpdec/fourstep.c
#         _decimal/libmpdec/io.c
#         _decimal/libmpdec/mpdecimal.c
#         _decimal/libmpdec/numbertheory.c
#         _decimal/libmpdec/sixstep.c
#         _decimal/libmpdec/transpose.c
#     )
#     if(PY_VERSION VERSION_GREATER_EQUAL "3.9")
#         list(APPEND _decimal_EXTRA_SOURCES
#             _decimal/libmpdec/mpalloc.c
#         )
#     else()
#         list(APPEND _decimal_EXTRA_SOURCES
#             _decimal/libmpdec/memory.c
#         )
#     endif()
#     if(WIN32)
#         if(${CMAKE_SIZEOF_VOID_P} EQUAL 8)
#             enable_language(ASM_MASM)
#             list(APPEND _decimal_EXTRA_SOURCES
#                 _decimal/libmpdec/vcdiv64.asm
#             )
#         endif()
#     endif()
#     set(_decimal_INCLUDEDIRS INCLUDEDIRS ${CPYTHON_SOURCE_DIR}/Modules/_decimal/libmpdec)
# endif()
# set(libmpdec_config )
# set(_decimal_compile_flags )
# if(APPLE)
#     # Universal here means: build with the same options Python
#     # was built with.
#     set(libmpdec_config universal)
# elseif(${CMAKE_SIZEOF_VOID_P} EQUAL 8)
#     if(HAVE_GCC_ASM_FOR_X64)
#         set(libmpdec_config x64)
#     elseif(HAVE_GCC_UINT128_T)
#         set(libmpdec_config uint128)
#     else()
#         set(libmpdec_config ansi64)
#     endif()
# elseif(${CMAKE_SIZEOF_VOID_P} EQUAL 4)
#     if(HAVE_GCC_ASM_FOR_X87 AND
#         (CMAKE_C_COMPILER_ID MATCHES "GNU" OR CMAKE_C_COMPILER_ID MATCHES "Clang")
#         AND NOT CMAKE_SYSTEM MATCHES SunOS
#     )
#         # solaris: problems with register allocation.
#         # icc >= 11.0 works as well.
#         set(libmpdec_config ppro)
#         list(APPEND _decimal_compile_flags "-Wno-unknown-pragmas")
#     else()
#       set(libmpdec_config ansi32)
#     endif()
# else()
#     message(FATAL_ERROR "extension_decimal: unsupported architecture")
# endif()

# # Workarounds for toolchain bugs:
# if(HAVE_IPA_PURE_CONST_BUG)
#     # Some versions of gcc miscompile inline asm:
#     # http://gcc.gnu.org/bugzilla/show_bug.cgi?id=46491
#     # http://gcc.gnu.org/ml/gcc/2010-11/msg00366.html
#     list(APPEND _decimal_compile_flags "-fno-ipa-pure-const")
# endif()
# if(HAVE_GLIBC_MEMMOVE_BUG)
#     # _FORTIFY_SOURCE wrappers for memmove and bcopy are incorrect:
#     # http://sourceware.org/ml/libc-alpha/2010-12/msg00009.html
#     # XXX Implement missing logic
# #    undef_macros.append('_FORTIFY_SOURCE')
# endif()

# if(NOT WITH_THREAD AND PY_VERSION VERSION_LESS "3.7")
#     # Faster version without thread local contexts:
#     list(APPEND libmpdec_config_${libmpdec_config}
#         WITHOUT_THREADS
#     )
# endif()

# if(WIN32)
#     list(APPEND libmpdec_config_${libmpdec_config}
#         _CRT_SECURE_NO_WARNINGS MASM
#     )
# endif()

# # Uncomment for extra functionality:
# #list(APPEND libmpdec_config_${libmpdec_config} EXTRA_FUNCTIONALITY)

# if(ENABLE_DECIMAL)
#     message(STATUS "extension_decimal: libmpdec_config [${libmpdec_config}]")
# endif()
# add_python_extension(_decimal
#     SOURCES
#         _decimal/_decimal.c
#         _decimal/docstrings.h
#         ${_decimal_EXTRA_SOURCES}
#     DEFINITIONS ${libmpdec_config_${libmpdec_config}}
#     ${_decimal_REQUIRES}
#     ${_decimal_LIBRARIES}
#     ${_decimal_INCLUDEDIRS}
# )
# if(_decimal_compile_flags AND ENABLE_DECIMAL AND NOT BUILTIN_DECIMAL)
#     set_target_properties(extension_decimal PROPERTIES COMPILE_FLAGS ${_decimal_compile_flags})
# endif()

# endif()

add_python_extension(pyexpat
    SOURCES pyexpat.c
    LIBRARIES Poco::XML
    INCLUDEDIRS ${proton_SOURCE_DIR}/base/poco/XML/src
)
add_python_extension(_elementtree
    SOURCES _elementtree.c
    LIBRARIES Poco::XML
)

# We always compile these even when OpenSSL is available (issue #14693).
# It's harmless and the object code is tiny (40-50 KB per module,
# only loaded when actually used).
add_python_extension(_md5 SOURCES md5module.c INCLUDEDIRS ${_HACL_INCLUDE} ${_HACL_DIR} LIBRARIES libpython_hacl)
add_python_extension(_sha1 SOURCES sha1module.c INCLUDEDIRS ${_HACL_INCLUDE} ${_HACL_DIR} LIBRARIES libpython_hacl)
# sha256/sha512 merged into _sha2 in CPython 3.14
add_python_extension(_sha2 SOURCES sha2module.c INCLUDEDIRS ${_HACL_INCLUDE} ${_HACL_DIR} LIBRARIES libpython_hacl)

# Extensions that depend on other libraries
add_python_extension(binascii
    SOURCES binascii.c
    DEFINITIONS USE_ZLIB_CRC32
    LIBRARIES ch_contrib::zlib
)

# FIXME: remove python2
# add_python_extension(_bsddb
#     REQUIRES IS_PY2 DB_INCLUDE_PATH DB_LIBRARY
#     SOURCES _bsddb.c
#     LIBRARIES ${DB_LIBRARIES}
#     INCLUDEDIRS ${DB_INCLUDE_PATH}
# )

# FIXME: BZIP2_LIBRARIES
set(bz2_2_NAME bz2)
set(bz2_3_NAME _bz2)
add_python_extension(${bz2_${PY_VERSION_MAJOR}_NAME}
    REQUIRES BZIP2_LIBRARIES BZIP2_INCLUDE_DIR
    SOURCES ${bz2_${PY_VERSION_MAJOR}_NAME}module.c
    LIBRARIES ch_contrib::bzip2
)

# FIXME: CURSES_LIBRARIES
# set(curses_common_REQUIRES CURSES_LIBRARIES)
# set(curses_common_LIBRARIES ${CURSES_LIBRARIES})
# if(WITH_STATIC_DEPENDENCIES)
#     list(APPEND curses_common_REQUIRES TINFO_LIBRARY GPM_LIBRARY)
#     list(APPEND curses_common_LIBRARIES ${TINFO_LIBRARY} ${GPM_LIBRARY})
# endif()
# add_python_extension(_curses_panel
#     REQUIRES ${curses_common_REQUIRES} PANEL_LIBRARIES "HAVE_PANEL_H OR HAVE_NCURSES_PANEL_H"
#     SOURCES _curses_panel.c
#     LIBRARIES ${curses_common_LIBRARIES} ${PANEL_LIBRARIES}
# )
# add_python_extension(_curses
#     REQUIRES ${curses_common_REQUIRES}
#     SOURCES _cursesmodule.c
#     LIBRARIES ${curses_common_LIBRARIES}
# )

# FIXME: GDBM_LIBRARY, GDBM_COMPAT_LIBRARY
# set(dbm2_SOURCES dbmmodule.c)
# set(dbm3_SOURCES _dbmmodule.c)
# if(IS_PY3)
#   set(dbm_name _dbm)
# else()
#   set(dbm_name dbm)
# endif()
# add_python_extension(${dbm_name}
#     REQUIRES NDBM_TAG GDBM_LIBRARY GDBM_COMPAT_LIBRARY
#     SOURCES ${dbm${PY_VERSION_MAJOR}_SOURCES}
#     DEFINITIONS HAVE_${NDBM_TAG}_H
#     LIBRARIES ${GDBM_LIBRARY} ${GDBM_COMPAT_LIBRARY}
#     INCLUDEDIRS ${${NDBM_TAG}_INCLUDE_PATH}
# )
# set(gdbm2_SOURCES gdbmmodule.c)
# set(gdbm3_SOURCES _gdbmmodule.c)
# if(IS_PY3)
#   set(gdbm_name _gdbm)
# else()
#   set(gdbm_name gdbm)
# endif()
# add_python_extension(${gdbm_name}
#     REQUIRES GDBM_INCLUDE_PATH GDBM_LIBRARY GDBM_COMPAT_LIBRARY
#     SOURCES ${gdbm${PY_VERSION_MAJOR}_SOURCES}
#     DEFINITIONS HAVE_GDBM_DASH_NDBM_H
#     LIBRARIES ${GDBM_LIBRARY}
#     INCLUDEDIRS ${GDBM_INCLUDE_PATH}
# )

add_python_extension(_hashlib
    SOURCES _hashopenssl.c
    LIBRARIES OpenSSL::SSL
)
if(ENABLE_HASHLIB AND CMAKE_C_COMPILER_ID MATCHES GNU)
    set_property(SOURCE ${CPYTHON_SOURCE_DIR}/Modules/_hashopenssl.c PROPERTY COMPILE_FLAGS -Wno-deprecated-declarations)
endif()

# FIXME: LZMA_LIBRARY
# # lzma module was introduced in Python 3.3
# set(is_py33_or_greater 0)
# if(IS_PY3 AND PY_VERSION_MINOR VERSION_GREATER 2)
#   set(is_py33_or_greater 1)
# endif()
add_python_extension(_lzma
    SOURCES _lzmamodule.c
    DEFINITIONS MODULE_NAME="lzma" LZMA_API_STATIC=1
    LIBRARIES ch_contrib::xz
)

# FIXME: READLINE_LIBRARY, CURSES_LIBRARIES
# add_python_extension(readline
#     REQUIRES READLINE_INCLUDE_PATH READLINE_LIBRARY CURSES_LIBRARIES HAVE_READLINE_READLINE_H
#     SOURCES readline.c
#     LIBRARIES ${READLINE_LIBRARY} ${CURSES_LIBRARIES}
#     INCLUDEDIRS ${READLINE_INCLUDE_PATH}
# )

add_python_extension(_sqlite3
    SOURCES _sqlite/blob.c
            _sqlite/connection.c
            _sqlite/cursor.c
            _sqlite/microprotocols.c
            _sqlite/module.c
            _sqlite/prepare_protocol.c
            _sqlite/row.c
            _sqlite/statement.c
            _sqlite/util.c
    DEFINITIONS MODULE_NAME="sqlite3" SQLITE_OMIT_LOAD_EXTENSION=1
    LIBRARIES ch_contrib::sqlite
)
if(ENABLE_SQLITE AND CMAKE_C_COMPILER_ID MATCHES GNU)
    set_property(SOURCE ${CPYTHON_SOURCE_DIR}/Modules/_sqlite/module.c PROPERTY COMPILE_FLAGS -Wno-deprecated-declarations)
endif()

set(_ssl_SOURCES _ssl.c)
add_python_extension(_ssl
    SOURCES ${_ssl_SOURCES}
    LIBRARIES OpenSSL::Crypto OpenSSL::SSL
)

# FIXME: TCL_LIBRARY, TK_LIBRARY
# set(_tkinter_REQUIRES TCL_LIBRARY TK_LIBRARY TCL_INCLUDE_PATH TK_INCLUDE_PATH)
# set(_tkinter_LIBRARIES ${TCL_LIBRARY} ${TK_LIBRARY})
# set(_tkinter_INCLUDEDIRS ${TCL_INCLUDE_PATH} ${TK_INCLUDE_PATH})
# if(UNIX)
#   list(APPEND _tkinter_REQUIRES X11_INCLUDE_DIR X11_LIBRARIES)
#   list(APPEND _tkinter_LIBRARIES ${X11_LIBRARIES})
#   list(APPEND _tkinter_INCLUDEDIRS ${X11_INCLUDE_DIR})
# endif()
# add_python_extension(_tkinter
#     REQUIRES ${_tkinter_REQUIRES}
#     SOURCES _tkinter.c tkappinit.c
#     DEFINITIONS WITH_APPINIT=1
#     LIBRARIES ${_tkinter_LIBRARIES}
#     INCLUDEDIRS ${_tkinter_INCLUDEDIRS}
# )

add_python_extension(zlib
    SOURCES zlibmodule.c
    DEFINITIONS USE_ZLIB_CRC32
    LIBRARIES ch_contrib::zlib
)

# if(USE_LIBEDIT AND ENABLE_READLINE AND NOT BUILTIN_READLINE)
#     set_target_properties(extension_readline PROPERTIES
#         COMPILE_DEFINITIONS "USE_LIBEDIT")
# endif()
