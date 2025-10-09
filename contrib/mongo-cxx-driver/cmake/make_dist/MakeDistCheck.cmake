include(MakeDistFiles)

function(RUN_DIST_CHECK PACKAGE_PREFIX EXT)
   set(tarball ${PACKAGE_PREFIX}.tar${EXT})

   if(NOT EXISTS ${tarball})
      message(FATAL_ERROR "Can't find dist tarball '${tarball}'")
   endif()

   # Remove the directory to which we're about to extract
   file(REMOVE_RECURSE ${PACKAGE_PREFIX})

   # Untar the distribution we want to check
   set(TAR_OPTION "zxf")

   if(${EXT} STREQUAL ".bz2")
      set(TAR_OPTION "jxf")
   endif()

   execute_process_and_check_result(
      COMMAND ${CMAKE_COMMAND} -E tar ${TAR_OPTION} ${tarball}
      WORKING_DIRECTORY .
      ERROR_MSG "Command to untar ${tarball} failed."
   )

   # Ensure a VERSION_CURRENT file is present.
   if(MONGOCXX_INCLUDE_VERSION_FILE_IN_DIST AND NOT EXISTS ${PACKAGE_PREFIX}/build/VERSION_CURRENT)
      message (FATAL_ERROR "Expected tarball to contain a `build/VERSION_CURRENT` file, but it does not")
   endif ()

   set(BUILD_DIR "_cmake_build")
   set(INSTALL_DIR "_cmake_install")
   file(REMOVE_RECURSE ${BUILD_DIR} ${INSTALL_DIR})

   file(MAKE_DIRECTORY ${BUILD_DIR} ${INSTALL_DIR})

   # Ensure distcheck inherits polyfill library selection.
   set(polyfill_flags "")

   if(NOT "${CMAKE_CXX_STANDARD}" STREQUAL "")
      list(APPEND polyfill_flags "-DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}")
   endif()

   if(NOT "${BOOST_ROOT}" STREQUAL "")
      list(APPEND polyfill_flags "-DBOOST_ROOT=${BOOST_ROOT}")
   endif()

   if(NOT "${BSONCXX_POLY_USE_MNMLSTC}" STREQUAL "")
      list(APPEND polyfill_flags "-DBSONCXX_POLY_USE_MNMLSTC=${BSONCXX_POLY_USE_MNMLSTC}")
   endif()

   if(NOT "${BSONCXX_POLY_USE_SYSTEM_MNMLSTC}" STREQUAL "")
      list(APPEND polyfill_flags "-DBSONCXX_POLY_USE_SYSTEM_MNMLSTC=${BSONCXX_POLY_USE_SYSTEM_MNMLSTC}")
   endif()

   if(NOT "${BSONCXX_POLY_USE_BOOST}" STREQUAL "")
      list(APPEND polyfill_flags "-DBSONCXX_POLY_USE_BOOST=${BSONCXX_POLY_USE_BOOST}")
   endif()

   if(NOT "${BSONCXX_POLY_USE_IMPLS}" STREQUAL "")
      list(APPEND polyfill_flags "-DBSONCXX_POLY_USE_IMPLS=${BSONCXX_POLY_USE_IMPLS}")
   endif()

   if(NOT "${BSONCXX_POLY_USE_STD}" STREQUAL "")
      list(APPEND polyfill_flags "-DBSONCXX_POLY_USE_STD=${BSONCXX_POLY_USE_STD}")
   endif()

   execute_process_and_check_result(
      COMMAND ${CMAKE_COMMAND} -E echo "Configuring distcheck with CMake flags: ${polyfill_flags}"
      WORKING_DIRECTORY .
      ERROR_MSG "Failed to echo polyfill flags"
   )

   execute_process_and_check_result(
      COMMAND ${CMAKE_COMMAND}
         -S ../${PACKAGE_PREFIX}
         -B .
         -DCMAKE_BUILD_TYPE=Release
         -DMONGOCXX_ENABLE_SLOW_TESTS=ON
         -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
         -DCMAKE_INSTALL_PREFIX=../${INSTALL_DIR}
         ${polyfill_flags}

      WORKING_DIRECTORY ${BUILD_DIR}
      ERROR_MSG "CMake configure command failed."
   )

   # Run make in the build directory
   separate_arguments(build_opts)
   execute_process_and_check_result(
      COMMAND ${CMAKE_COMMAND} --build .
      WORKING_DIRECTORY ${BUILD_DIR}
      ERROR_MSG "Make build failed."
   )

   # Run make install
   execute_process_and_check_result(
      COMMAND ${CMAKE_COMMAND} --build . --target install
      WORKING_DIRECTORY ${BUILD_DIR}
      ERROR_MSG "Make install failed."
   )

   # Run make dist
   execute_process_and_check_result(
      COMMAND ${CMAKE_COMMAND} --build . --target dist
      WORKING_DIRECTORY ${BUILD_DIR}
      ERROR_MSG "Make dist failed."
   )

   message("distcheck complete.")
endfunction()

run_dist_check(${PACKAGE_PREFIX} .gz)
