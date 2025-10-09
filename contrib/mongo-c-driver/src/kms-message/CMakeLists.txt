cmake_minimum_required (VERSION 3.5)
project (kms_message
   VERSION 0.0.1
   LANGUAGES C
)

set (CMAKE_C_STANDARD 90)

include (CheckCCompilerFlag)
# All targets obey visibility, not just library targets.
cmake_policy (SET CMP0063 NEW)
set (CMAKE_C_VISIBILITY_PRESET hidden)
set (KMS_MESSAGE_SOURCES
   src/kms_b64.c
   src/kms_message/kms_b64.h
   src/hexlify.c
   src/hexlify.h
   src/kms_azure_request.c
   src/kms_crypto.h
   src/kms_crypto_none.c
   src/kms_crypto_windows.c
   src/kms_crypto_apple.c
   src/kms_crypto_libcrypto.c
   src/kms_caller_identity_request.c
   src/kms_decrypt_request.c
   src/kms_encrypt_request.c
   src/kms_gcp_request.c
   src/kms_kmip_reader_writer.c
   src/kms_kmip_request.c
   src/kms_kmip_response.c
   src/kms_kmip_response_parser.c
   src/kms_kv_list.c
   src/kms_kv_list.h
   src/kms_message.c
   src/kms_port.c
   src/kms_message/kms_azure_request.h
   src/kms_message/kms_caller_identity_request.h
   src/kms_message/kms_decrypt_request.h
   src/kms_message/kms_encrypt_request.h
   src/kms_message/kms_gcp_request.h
   src/kms_message/kms_message.h
   src/kms_message/kms_request.h
   src/kms_message/kms_request_opt.h
   src/kms_message/kms_response.h
   src/kms_message/kms_response_parser.h
   src/kms_request.c
   src/kms_request_opt.c
   src/kms_request_opt_private.h
   src/kms_request_str.c
   src/kms_request_str.h
   src/kms_response.c
   src/kms_response_parser.c
   src/sort.c
   )

if (DISABLE_NATIVE_CRYPTO)
   # Nothing
elseif (WIN32)
   set (KMS_MESSAGE_DEFINITIONS
         ${KMS_MESSAGE_DEFINITIONS}
         "KMS_MESSAGE_ENABLE_CRYPTO"
         "KMS_MESSAGE_ENABLE_CRYPTO_CNG")
elseif (APPLE)
   set (KMS_MESSAGE_DEFINITIONS ${KMS_MESSAGE_DEFINITIONS}
         "KMS_MESSAGE_ENABLE_CRYPTO"
         "KMS_MESSAGE_ENABLE_CRYPTO_COMMON_CRYPTO")
else()
   set (KMS_MESSAGE_DEFINITIONS
         ${KMS_MESSAGE_DEFINITIONS}
         "KMS_MESSAGE_ENABLE_CRYPTO"
         "KMS_MESSAGE_ENABLE_CRYPTO_LIBCRYPTO")
endif()

include (CheckSymbolExists)
CHECK_SYMBOL_EXISTS (gmtime_r time.h KMS_MESSAGE_HAVE_GMTIME_R)
if (KMS_MESSAGE_HAVE_GMTIME_R)
   set (KMS_MESSAGE_DEFINITIONS
         ${KMS_MESSAGE_DEFINITIONS}
         "KMS_MESSAGE_HAVE_GMTIME_R")
endif ()

include (TestBigEndian)
TEST_BIG_ENDIAN (KMS_BIG_ENDIAN)
if (KMS_BIG_ENDIAN)
   message ("Detected byte order: big endian")
   set (KMS_MESSAGE_DEFINITIONS ${KMS_MESSAGE_DEFINITIONS} "KMS_MESSAGE_BIG_ENDIAN")
else ()
   message ("Detected byte order: little endian")
   set (KMS_MESSAGE_DEFINITIONS ${KMS_MESSAGE_DEFINITIONS} "KMS_MESSAGE_LITTLE_ENDIAN")
endif ()

add_library (
   kms_message SHARED
   ${KMS_MESSAGE_SOURCES}
)

add_library (
   kms_message_static STATIC
   ${KMS_MESSAGE_SOURCES}
)

string(FIND "${CMAKE_C_FLAGS}" "-fPIC" FPIC_LOCATION)
if (NOT WIN32 AND ENABLE_PIC AND "${FPIC_LOCATION}" EQUAL "-1")
   target_compile_options (kms_message_static PUBLIC -fPIC)
   message ("Adding -fPIC to compilation of kms_message_static components")
endif ()

add_library (
   kms_message_obj OBJECT
   ${KMS_MESSAGE_SOURCES}
)

target_compile_definitions (kms_message_obj PRIVATE ${KMS_MESSAGE_DEFINITIONS})

if (NOT DISABLE_NATIVE_CRYPTO)
   if (WIN32)
      target_link_libraries(kms_message "bcrypt" "crypt32")
      target_link_libraries(kms_message_static "bcrypt" "crypt32")
   elseif (APPLE)
      target_link_libraries (kms_message "-framework Security -framework CoreFoundation")
      target_link_libraries (kms_message_static "-framework Security -framework CoreFoundation")
   else()
      include (FindOpenSSL)
      target_link_libraries(kms_message "${OPENSSL_LIBRARIES}")
      target_include_directories(kms_message SYSTEM PRIVATE "${OPENSSL_INCLUDE_DIR}")
      target_link_libraries(kms_message_static "${OPENSSL_LIBRARIES}")
      target_include_directories(kms_message_static SYSTEM PRIVATE "${OPENSSL_INCLUDE_DIR}")
   endif()
endif ()

if ( CMAKE_COMPILER_IS_GNUCC )
    set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -Wall -Wno-unused-function -Wsign-compare")
endif()
if ( MSVC )
    # W4273 - inconsistent dll linkage
    # W4996 - POSIX name for this item is deprecated
    set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} /W3 /wd4273 /wd4996 /D_CRT_SECURE_NO_WARNINGS")
endif()

set_target_properties (kms_message PROPERTIES
   SOVERSION 0
   VERSION "0.0.0"
   OUTPUT_NAME "kms_message"
)

set_target_properties (kms_message_static PROPERTIES
   SOVERSION 0
   VERSION "0.0.0"
   OUTPUT_NAME "kms_message-static"
)

target_compile_definitions (kms_message PRIVATE KMS_MSG_COMPILATION)
target_compile_definitions (kms_message PRIVATE ${KMS_MESSAGE_DEFINITIONS})
set_property (TARGET kms_message APPEND PROPERTY
   COMPATIBLE_INTERFACE_STRING kms_message_MAJOR_VERSION
)

target_compile_definitions (kms_message_static PRIVATE KMS_MSG_COMPILATION)
target_compile_definitions (kms_message_static PRIVATE ${KMS_MESSAGE_DEFINITIONS})
target_compile_definitions (kms_message_static PUBLIC KMS_MSG_STATIC)
set_property (TARGET kms_message_static APPEND PROPERTY
   COMPATIBLE_INTERFACE_STRING kms_message_MAJOR_VERSION
   )

include (CMakePackageConfigHelpers)
if (ENABLE_STATIC)
   set (TARGETS_TO_INSTALL kms_message kms_message_static)
else ()
   set (TARGETS_TO_INSTALL kms_message)
endif ()
install (
   TARGETS ${TARGETS_TO_INSTALL}
   EXPORT kms_message_targets
   LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
   ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
   RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
   INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install (
   FILES
   src/kms_message/kms_azure_request.h
   src/kms_message/kms_b64.h
   src/kms_message/kms_caller_identity_request.h
   src/kms_message/kms_decrypt_request.h
   src/kms_message/kms_encrypt_request.h
   src/kms_message/kms_gcp_request.h
   src/kms_message/kms_message.h
   src/kms_message/kms_message_defines.h
   src/kms_message/kms_kmip_request.h
   src/kms_message/kms_kmip_response.h
   src/kms_message/kms_kmip_response_parser.h
   src/kms_message/kms_request.h
   src/kms_message/kms_request_opt.h
   src/kms_message/kms_response.h
   src/kms_message/kms_response_parser.h
   DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kms_message
   COMPONENT Devel
)

include (CMakePackageConfigHelpers)
write_basic_package_version_file (
   "${CMAKE_CURRENT_BINARY_DIR}/kms_message/kms_message-config-version.cmake"
   VERSION 0.0.1
   COMPATIBILITY AnyNewerVersion
)

export (EXPORT kms_message_targets
   NAMESPACE mongo::
   FILE "${CMAKE_CURRENT_BINARY_DIR}/kms_message/kms_message_targets.cmake"
)

configure_file (cmake/kms_message-config.cmake
   "${CMAKE_CURRENT_BINARY_DIR}/kms_message/kms_message-config.cmake"
   COPYONLY
)

set (ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/kms_message)
install (EXPORT kms_message_targets
   NAMESPACE mongo::
   FILE kms_message_targets.cmake
   DESTINATION ${ConfigPackageLocation}
)

install (
   FILES
   cmake/kms_message-config.cmake
   "${CMAKE_CURRENT_BINARY_DIR}/kms_message/kms_message-config-version.cmake"
   DESTINATION ${ConfigPackageLocation}
   COMPONENT Devel
)

# pkg-config.
set (PKG_CONFIG_LIBDIR "\${prefix}/${CMAKE_INSTALL_LIBDIR}")
set (PKG_CONFIG_INCLUDEDIR "\${prefix}/${CMAKE_INSTALL_INCLUDEDIR}")
set (PKG_CONFIG_LIBS "-L\${libdir} -lkms_message")
set (PKG_CONFIG_CFLAGS "-I\${includedir}")
configure_file (
   "${CMAKE_CURRENT_SOURCE_DIR}/cmake/libkms_message.pc.in"
   "${CMAKE_CURRENT_BINARY_DIR}/libkms_message.pc"
)

install (
   FILES "${CMAKE_CURRENT_BINARY_DIR}/libkms_message.pc"
   DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

# cannot run tests without crypto
if (NOT DISABLE_NATIVE_CRYPTO)
   add_executable (
      test_kms_request
      ${KMS_MESSAGE_SOURCES}
      test/test_kms_request.c
      test/test_kmip_reader_writer.c
      test/test_kms_util.c
      test/test_kms_kmip_request.c
      test/test_kms_kmip_response_parser.c
      test/test_kms_kmip_response.c
   )
   target_include_directories(test_kms_request PRIVATE ${PROJECT_SOURCE_DIR}/src)
   target_compile_definitions(test_kms_request PRIVATE ${KMS_MESSAGE_DEFINITIONS})

   if (WIN32)
      target_link_libraries(test_kms_request "bcrypt" "crypt32")
   elseif (APPLE)
      target_link_libraries (test_kms_request "-framework Security -framework CoreFoundation")
   else()
      include (FindOpenSSL)
      target_link_libraries(test_kms_request "${OPENSSL_LIBRARIES}")
      target_include_directories(test_kms_request SYSTEM PRIVATE "${OPENSSL_INCLUDE_DIR}")
   endif()

   add_test (
      NAME test_kms_request
      COMMAND test_kms_request
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
   )
endif ()

# build online_tests if OpenSSL is available (to create TLS connections).
if(DISABLE_NATIVE_CRYPTO)
   message ("test_kms_azure_online target disabled: Not building with native crypto.")
elseif(ENABLE_ONLINE_TESTS)
   add_executable(test_kms_azure_online test/test_kms_azure_online.c test/test_kms_util.c test/test_kms_online_util.c)
   target_include_directories(test_kms_azure_online PRIVATE ${PROJECT_SOURCE_DIR}/src)
   target_compile_definitions(test_kms_azure_online PRIVATE ${KMS_MESSAGE_DEFINITIONS})
   target_link_libraries(test_kms_azure_online _mongocrypt::mongoc)
   target_link_libraries(test_kms_azure_online kms_message_static)
   add_test (
      NAME test_kms_azure_online
      COMMAND test_kms_azure_online
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
   )

   add_executable(test_kms_gcp_online test/test_kms_gcp_online.c test/test_kms_util.c test/test_kms_online_util.c)
   target_include_directories(test_kms_gcp_online PRIVATE ${PROJECT_SOURCE_DIR}/src)
   target_compile_definitions(test_kms_gcp_online PRIVATE ${KMS_MESSAGE_DEFINITIONS})
   target_link_libraries(test_kms_gcp_online _mongocrypt::mongoc)
   target_link_libraries(test_kms_gcp_online kms_message_static)
   add_test (
      NAME test_kms_gcp_online
      COMMAND test_kms_gcp_online
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
   )
   set_tests_properties (
      test_kms_gcp_online test_kms_azure_online
      PROPERTIES
      SKIP_REGULAR_EXPRESSION "@@ctest-skip@@"
      # Older CMake (<3.16) does not support skipping via regex, so we'll use the
      # less-expressive skip-retcode.
      SKIP_RETURN_CODE 2
   )
endif ()
