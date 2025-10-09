INCLUDE_DIRECTORIES(${CC_SOURCE_DIR}/include)

IF(WIN32)
  SET_VERSION_INFO("TARGET:mariadb_client_plugin_info"
                   "FILE_TYPE:VFT_APP"
                   "SOURCE_FILE:client/ma_plugin_info.c"
                   "ORIGINAL_FILE_NAME:mariadb_client_plugin_info.exe"
                   "FILE_DESCRIPTION:Client plugin viewer")
ENDIF()

ADD_EXECUTABLE(mariadb_client_plugin_info ${mariadb_client_plugin_info_RC} ma_plugin_info.c)
TARGET_LINK_LIBRARIES(mariadb_client_plugin_info mariadbclient)

INSTALL(TARGETS mariadb_client_plugin_info
        DESTINATION ${INSTALL_BINDIR}
        COMPONENT SharedLibraries)
SIGN_TARGET(mariadb_client_plugin_info)
