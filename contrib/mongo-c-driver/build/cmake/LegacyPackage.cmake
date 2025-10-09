include (CMakePackageConfigHelpers)

# These aren't pkg-config files, they're CMake package configuration files.
function (install_package_config_file prefix)
   foreach (suffix "config.cmake")
      configure_package_config_file (
         ${CMAKE_CURRENT_LIST_DIR}/${PROJECT_NAME}-${prefix}-${suffix}.in
         ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-${prefix}-${suffix}
         INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}-${prefix}
      )

      install (
         FILES
            ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-${prefix}-${suffix}
         DESTINATION
            ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}-${prefix}
      )
   endforeach ()
   write_basic_package_version_file(
      ${PROJECT_NAME}-${prefix}-config-version.cmake
      VERSION "${PROJECT_VERSION}"
      COMPATIBILITY SameMajorVersion
   )
   install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-${prefix}-config-version.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}-${prefix})
endfunction ()

install_package_config_file ("1.0")

if (ENABLE_STATIC)
   install_package_config_file ("static-1.0")
endif ()
