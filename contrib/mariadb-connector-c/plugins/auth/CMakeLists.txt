SET(AUTH_DIR ${CC_SOURCE_DIR}/plugins/auth)

INCLUDE_DIRECTORIES(${AUTH_DIR})
INCLUDE_DIRECTORIES(${CC_SOURCE_DIR}/include)

SET(CRYPTO_PLUGIN 1)
IF(WIN32)
  SET(CRYPT_SOURCE ${CC_SOURCE_DIR}/libmariadb/secure/win_crypt.c)
  SET(CRYPT_LIBS crypt32)
ELSEIF(WITH_SSL STREQUAL "OPENSSL")
  SET(CRYPT_SOURCE ${CC_SOURCE_DIR}/libmariadb/secure/openssl_crypt.c)
  SET(CRYPT_LIBS ${SSL_LIBRARIES})
ELSEIF(WITH_SSL STREQUAL "GNUTLS")
  SET(CRYPT_SOURCE ${CC_SOURCE_DIR}/libmariadb/secure/gnutls_crypt.c)
  IF (NOT SSL_LIBRARIES MATCHES "^.*\\.a$")
    SET(CRYPT_LIBS ${SSL_LIBRARIES})
  ENDIF()
ELSE()
  UNSET(CRYPTO_PLUGIN)
ENDIF()

#native password
REGISTER_PLUGIN(TARGET mysql_native_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                CONFIGURATIONS STATIC
                DEFAULT STATIC
                SOURCES ${CC_SOURCE_DIR}/plugins/auth/my_auth.c)

#Dialog client authentication plugin
REGISTER_PLUGIN(TARGET dialog
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                CONFIGURATIONS DYNAMIC STATIC OFF
                DEFAULT DYNAMIC
                SOURCES ${CC_SOURCE_DIR}/plugins/auth/dialog.c
                        ${CC_SOURCE_DIR}/libmariadb/get_password.c)


# Crypto plugins
IF(CRYPTO_PLUGIN)

  #ED25519 client authentication plugin
  SET(REF10_DIR ${CC_SOURCE_DIR}/plugins/auth/ref10)
  SET(REF10_SOURCES ${REF10_DIR}/fe_0.c ${REF10_DIR}/fe_isnegative.c ${REF10_DIR}/fe_sub.c ${REF10_DIR}/ge_p1p1_to_p2.c
                    ${REF10_DIR}/ge_p3_to_cached.c ${REF10_DIR}/open.c ${REF10_DIR}/fe_1.c ${REF10_DIR}/fe_isnonzero.c
                    ${REF10_DIR}/fe_tobytes.c ${REF10_DIR}/ge_p1p1_to_p3.c ${REF10_DIR}/ge_p3_to_p2.c ${REF10_DIR}/sc_muladd.c
                    ${REF10_DIR}/fe_add.c ${REF10_DIR}/fe_mul.c ${REF10_DIR}/ge_add.c ${REF10_DIR}/ge_p2_0.c ${REF10_DIR}/ge_precomp_0.c
                    ${REF10_DIR}/sc_reduce.c ${REF10_DIR}/fe_cmov.c ${REF10_DIR}/fe_neg.c ${REF10_DIR}/ge_double_scalarmult.c 
                    ${REF10_DIR}/ge_p2_dbl.c ${REF10_DIR}/ge_scalarmult_base.c ${REF10_DIR}/sign.c ${REF10_DIR}/fe_copy.c
                    ${REF10_DIR}/fe_pow22523.c ${REF10_DIR}/ge_frombytes.c ${REF10_DIR}/ge_p3_0.c ${REF10_DIR}/ge_sub.c
                    ${REF10_DIR}/verify.c ${REF10_DIR}/fe_frombytes.c ${REF10_DIR}/fe_sq2.c ${REF10_DIR}/ge_madd.c
                    ${REF10_DIR}/ge_p3_dbl.c ${REF10_DIR}/ge_tobytes.c ${REF10_DIR}/fe_invert.c ${REF10_DIR}/fe_sq.c
                    ${REF10_DIR}/ge_msub.c ${REF10_DIR}/ge_p3_tobytes.c ${REF10_DIR}/keypair.c)

  REGISTER_PLUGIN(TARGET client_ed25519
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                CONFIGURATIONS DYNAMIC STATIC OFF
                DEFAULT DYNAMIC
                SOURCES ${CC_SOURCE_DIR}/plugins/auth/ed25519.c 
                        ${REF10_SOURCES}
                        ${CRYPT_SOURCE}
                INCLUDES ${REF10_DIR}
                LIBRARIES ${CRYPT_LIBS}
                COMPILE_OPTIONS -DMYSQL_CLIENT=1)
  IF(MSVC)
     # Silence conversion (integer truncantion) warnings from reference code
     SET_SOURCE_FILES_PROPERTIES(${REF10_SOURCES} PROPERTY COMPILE_FLAGS "-DMYSQL_CLIENT=1 /wd4244 /wd4146")
  ENDIF()

  # SHA256 caching plugin for MySQL 8.0 connection
  REGISTER_PLUGIN(TARGET caching_sha2_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                CONFIGURATIONS DYNAMIC STATIC OFF
                DEFAULT DYNAMIC
                SOURCES ${CC_SOURCE_DIR}/plugins/auth/caching_sha2_pw.c 
                        ${CRYPT_SOURCE}
                LIBRARIES ${CRYPT_LIBS})

  IF(WITH_SSL STREQUAL "GNUTLS" AND NOT WIN32)
    MESSAGE1(STATUS "sha256_password not supported by GnuTLS due to missing OAEP padding")
  ELSE()
    REGISTER_PLUGIN(TARGET sha256_password
                    TYPE MARIADB_CLIENT_PLUGIN_AUTH
                    CONFIGURATIONS DYNAMIC STATIC OFF
                    DEFAULT DYNAMIC
                    SOURCES ${AUTH_DIR}/sha256_pw.c
                    LIBRARIES ${CRYT_LIBS})
  ENDIF()
ENDIF()

#GSSAPI client authentication plugin
IF(NOT WIN32)
  INCLUDE(${CC_SOURCE_DIR}/cmake/FindGSSAPI.cmake)
ENDIF()

#GSSAPI client authentication plugin
IF(NOT WIN32)
  INCLUDE(${CC_SOURCE_DIR}/cmake/FindGSSAPI.cmake)
  IF(GSSAPI_FOUND)
    SET(GSSAPI_SOURCES ${AUTH_DIR}/auth_gssapi_client.c ${AUTH_DIR}/gssapi_client.c ${AUTH_DIR}/gssapi_errmsg.c)
  ENDIF()
ELSE()
  SET(GSSAPI_LIBS secur32)
  SET(GSSAPI_SOURCES ${AUTH_DIR}/auth_gssapi_client.c ${AUTH_DIR}/sspi_client.c ${AUTH_DIR}/sspi_errmsg.c)
ENDIF()
IF(GSSAPI_SOURCES)
  REGISTER_PLUGIN(TARGET auth_gssapi_client
                  TYPE MARIADB_CLIENT_PLUGIN_AUTH
                  CONFIGURATIONS DYNAMIC STATIC OFF
                  DEFAULT DYNAMIC
                  SOURCES ${GSSAPI_SOURCES}
                  INCLUDES ${CC_SOURCE_DIR}/plugins/auth ${GSSAPI_INCS}
                  LIBRARIES ${GSSAPI_LIBS})
ENDIF()

# old_password plugin
REGISTER_PLUGIN(TARGET mysql_old_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                CONFIGURATIONS STATIC DYNAMIC OFF
                DEFAULT STATIC
                SOURCES ${AUTH_DIR}/old_password.c)

# Cleartext
REGISTER_PLUGIN(TARGET mysql_clear_password
                TYPE MARIADB_CLIENT_PLUGIN_AUTH
                CONFIGURATIONS DYNAMIC STATIC OFF
                DEFAULT DYNAMIC
                SOURCES ${AUTH_DIR}/mariadb_cleartext.c)

