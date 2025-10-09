IF (WITH_CURL)
  INCLUDE(FindCURL)
  IF(CURL_FOUND)

    ADD_DEFINITIONS(-DHAVE_REMOTEIO=1)
    #remote io plugin
    REGISTER_PLUGIN(TARGET remote_io
                  TYPE MARIADB_CLIENT_PLUGIN_IO
                  CONFIGURATIONS DYNAMIC STATIC OFF
                  DEFAULT DYNAMIC
                  SOURCES ${CC_SOURCE_DIR}/plugins/io/remote_io.c
                  INCLUDES ${CURL_INCLUDE_DIR}
                  LIBRARIES ${CURL_LIBRARIES})
  ENDIF()
ENDIF()
