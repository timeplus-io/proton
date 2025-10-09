#
#  Copyright (C) 2013-2018 MariaDB Corporation AB
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the COPYING-CMAKE-SCRIPTS file.
#
# plugin configuration

include(${CC_SOURCE_DIR}/cmake/install_plugins.cmake)
include(${CC_SOURCE_DIR}/cmake/sign.cmake)

FUNCTION(REGISTER_PLUGIN)

  SET(one_value_keywords TARGET DEFAULT TYPE)
  SET(multi_value_keywords CONFIGURATIONS SOURCES LIBRARIES INCLUDES COMPILE_OPTIONS)

  cmake_parse_arguments(CC_PLUGIN
                        "${options}"
                        "${one_value_keywords}"
                        "${multi_value_keywords}"
                        ${ARGN})

  # overwrite default if it was specified with cmake option
  string(TOUPPER ${CC_PLUGIN_TARGET} cc_plugin)
  if(NOT "${CLIENT_PLUGIN_${cc_plugin}}" STREQUAL "")
    SET(CC_PLUGIN_DEFAULT ${CLIENT_PLUGIN_${cc_plugin}})
  endif()

  # use uppercase
  string(TOUPPER ${CC_PLUGIN_TARGET} target_name)
  string(TOUPPER "${CC_PLUGIN_CONFIGURATIONS}" CC_PLUGIN_CONFIGURATIONS)

  if(NOT ${PLUGIN_${target_name}} STREQUAL "")
    string(TOUPPER ${PLUGIN_${target_name}} PLUGIN_${target_name})
    set(CC_PLUGIN_DEFAULT ${PLUGIN_${target_name}})
  endif()

# check if default value is valid
  string(TOUPPER ${CC_PLUGIN_DEFAULT} CC_PLUGIN_DEFAULT)
  list(FIND CC_PLUGIN_CONFIGURATIONS ${CC_PLUGIN_DEFAULT} configuration_found)
  if(${configuration_found} EQUAL -1)
    message(FATAL_ERROR "Invalid plugin type ${CC_PLUGIN_DEFAULT}. Allowed plugin types are ${CC_PLUGIN_CONFIGURATIONS}")
  endif()

  if(NOT ${CC_PLUGIN_DEFAULT} STREQUAL "OFF")
    set(PLUGIN_${CC_PLUGIN_TARGET}_TYPE ${CC_PLUGIN_TYPE})

    if(${CC_PLUGIN_DEFAULT} STREQUAL "DYNAMIC")
      set_source_files_properties(${CC_PLUGIN_SOURCES}
                                 PROPERTIES COMPILE_FLAGS
                                 "-DPLUGIN_DYNAMIC=1 ${CC_PLUGIN_COMPILE_OPTIONS}")
      set(PLUGINS_DYNAMIC ${PLUGINS_DYNAMIC} ${CC_PLUGIN_TARGET} PARENT_SCOPE)
      if(WIN32)
        set(target ${CC_PLUGIN_TARGET})
        set(FILE_TYPE "VFT_DLL")
        set(FILE_DESCRIPTION "MariaDB client plugin")
        set(FILE_VERSION ${CPACK_PACKAGE_VERSION})
        set(ORIGINAL_FILE_NAME "${target}.dll")
        configure_file(${CC_SOURCE_DIR}/win/resource.rc.in
                       ${CC_BINARY_DIR}/win/${target}.rc
                       @ONLY)
        set(CC_PLUGIN_SOURCES ${CC_PLUGIN_SOURCES} ${CC_BINARY_DIR}/win/${target}.rc ${CC_SOURCE_DIR}/plugins/plugin.def)
      endif()
      add_library(${CC_PLUGIN_TARGET} MODULE ${CC_PLUGIN_SOURCES})
      target_link_libraries(${CC_PLUGIN_TARGET} ${CC_PLUGIN_LIBRARIES})
      set_target_properties(${CC_PLUGIN_TARGET} PROPERTIES PREFIX "")
      if (NOT "${CC_PLUGIN_INCLUDES}" STREQUAL "")
        if(CMAKE_VERSION VERSION_LESS 2.8.11)
          include_directories(${CC_PLUGIN_INCLUDES})
        else()
          target_include_directories(${CC_PLUGIN_TARGET} PRIVATE  ${CC_PLUGIN_INCLUDES})
        endif()
      endif()
      if (${CC_TARGET_COMPILE_OPTIONS})
        target_compile_options(${CC_PLUGIN_TARGET} ${CC_TARGET_COMPILE_OPTIONS})
      endif()

      if(WIN32)
        SIGN_TARGET(${target})
      endif()
      INSTALL_PLUGIN(${CC_PLUGIN_TARGET} ${CMAKE_CURRENT_BINARY_DIR})
    elseif(${CC_PLUGIN_DEFAULT} STREQUAL "STATIC")
      set(PLUGINS_STATIC ${PLUGINS_STATIC} ${CC_PLUGIN_TARGET} PARENT_SCOPE)
      set(LIBMARIADB_PLUGIN_CFLAGS ${LIBMARIADB_PLUGIN_CFLAGS} ${CC_PLUGIN_COMPILE_OPTIONS} PARENT_SCOPE)
      set(LIBMARIADB_PLUGIN_INCLUDES ${LIBMARIADB_PLUGIN_INCLUDES} ${CC_PLUGIN_INCLUDES} PARENT_SCOPE)
      set(LIBMARIADB_PLUGIN_SOURCES ${LIBMARIADB_PLUGIN_SOURCES} ${CC_PLUGIN_SOURCES} PARENT_SCOPE)
      set(LIBMARIADB_PLUGIN_LIBS ${LIBMARIADB_PLUGIN_LIBS} ${CC_PLUGIN_LIBRARIES} PARENT_SCOPE)
    endif()
  else()
    set(PLUGINS_OFF ${PLUGINS_OFF} ${CC_PLUGIN_TARGET})
  endif()
endfunction()
