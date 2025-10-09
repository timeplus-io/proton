# This module implements the process of making source distribution tarballs.
# The list of all non-generated files to include in the distribution is
# 'dist_manifest.txt'. Inclusion of generated files (e.g., documentation) is
# handled via the 'dist_generated' CMake cache variable.
#
# The procedure is:
#
# 1. Remove any existing dist directory and make a new one.
# 2. Copy all of the files in dist_manifest.text and ${dist_generated}
#    into the dist directory.
# 3. Create the tarball and compress it.
# 4. Remove the dist directory.

include (MakeDistFiles)


function (MAKE_DIST PACKAGE_PREFIX MONGOCXX_SOURCE_DIR BUILD_SOURCE_DIR)

   set (CMAKE_COMMAND_TMP "")
   set (CMAKE_COMMAND_TMP ${CMAKE_COMMAND} -E env)

   # -- Remove any existing packaging directory.

   file (REMOVE_RECURSE ${PACKAGE_PREFIX})

   if (EXISTS ${PACKAGE_PREFIX})
      message (FATAL_ERROR
         "Unable to remove existing dist directory \"${PACKAGE_PREFIX}\". Cannot continue."
      )
   endif ()

   # -- Copy in distributed files

   if (NOT EXISTS dist_manifest.txt)
      message (FATAL_ERROR "Cannot find dist manifest: dist_manifest.txt")
   endif ()

   file (STRINGS dist_manifest.txt ALL_DIST)

   foreach (file ${ALL_DIST})
      if (NOT EXISTS ${MONGOCXX_SOURCE_DIR}/${file})
         message (FATAL_ERROR
            "Can't find dist file ${MONGOCXX_SOURCE_DIR}/${file}"
         )
      endif ()
      get_filename_component (dir ${file} DIRECTORY)
      file (MAKE_DIRECTORY ${PACKAGE_PREFIX}/${dir})
      file (COPY
         ${MONGOCXX_SOURCE_DIR}/${file}
         DESTINATION
         ${PACKAGE_PREFIX}/${dir}
      )
   endforeach ()

   # -- Copy in build products that are distributed.

   foreach (file ${dist_generated})
      execute_process (COMMAND
         ${CMAKE_COMMAND} -E copy ${BUILD_SOURCE_DIR}/${file} ${PACKAGE_PREFIX}/${file}
      )
      if (NOT EXISTS ${PACKAGE_PREFIX}/${file})
         message (FATAL_ERROR
            "Copy of ${BUILD_SOURCE_DIR}/${file} to dist dir '${PACKAGE_PREFIX}' failed."
         )
      endif ()
   endforeach ()

   cmake_policy (SET CMP0012 NEW)

   # -- Create the tarball.

   execute_process_and_check_result (COMMAND
      ${CMAKE_COMMAND} -E tar cf ${PACKAGE_PREFIX}.tar ${PACKAGE_PREFIX}
      WORKING_DIRECTORY .
      ERROR_MSG "tar command to create ${PACKAGE_PREFIX}.tar failed."
   )

   # -- Compress the tarball with gzip

   execute_process_and_check_result (COMMAND
      ${CMAKE_COMMAND_TMP} gzip -f ${PACKAGE_PREFIX}.tar
      WORKING_DIRECTORY .
      ERROR_MSG "gzip command to create ${PACKAGE_PREFIX}.tar.gz failed."
   )

   # -- Clean up packaging directory.

   file (REMOVE_RECURSE ${PACKAGE_PREFIX})

   if (EXISTS ${PACKAGE_PREFIX})
      message (WARNING
         "Could not remove packaging directory '${PACKAGE_PREFIX}'."
      )
   endif ()

   # -- All done.

   message ("\n\nDistribution ${PACKAGE_PREFIX}.tar.gz created.\n\n")
endfunction ()

make_dist (${PACKAGE_PREFIX} ${MONGOCXX_SOURCE_DIR} ${BUILD_SOURCE_DIR})
