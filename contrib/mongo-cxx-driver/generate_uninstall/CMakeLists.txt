# Fabricate our own copy of the install manifest, since the installation has not
# generated the final version yet at this point

# NOTE: The CODE installed by this script contains a work-around for a bug in
# CMake that results in empty directories being installed.  The work-around
# involves pruning the empty directories from the install tree before generating
# the uninstall program.  For non-Windows platforms, this requires a simple call
# to the 'find' utility.  For Windows platforms, however, some creative jumping
# through hoops is needed.  Specifically, a short batch script iterates through
# all directories in the install tree, calling 'rmdir' on each.  This removes
# empty directories only and warns for non-empty directories (which warnings are
# suppressed by the ERROR_QUIET option, since they are spurious for this use
# case).

set (UNINSTALL_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

if (WIN32)
   string (REPLACE "/" "\\\\" CMAKE_INSTALL_PREFIX_WIN32
      "${CMAKE_INSTALL_PREFIX}"
   )
   install (CODE "
      string(REPLACE \";\" \"\\n\" MONGOCXX_INSTALL_MANIFEST_CONTENT
         \"\${CMAKE_INSTALL_MANIFEST_FILES}\")
      string(REPLACE \"/\" \"\\\\\" MONGOCXX_INSTALL_MANIFEST_CONTENT_WIN32
         \"\${MONGOCXX_INSTALL_MANIFEST_CONTENT}\")
      file(WRITE \"mongocxx_install_manifest.txt\"
         \"\${MONGOCXX_INSTALL_MANIFEST_CONTENT_WIN32}\")
      execute_process (
         COMMAND
            ${CMAKE_COMMAND} -E env
            cmd.exe /c
            \" for /f \" delims= \" %d in ('dir ${CMAKE_INSTALL_PREFIX_WIN32} /s /b /ad ^| C:\\\\Windows\\\\System32\\\\sort.exe /r') do rmdir %d \"
         OUTPUT_QUIET
         ERROR_QUIET
      )
      execute_process (
         COMMAND
            ${CMAKE_COMMAND} -E env
            cmd.exe /c
            \"${PROJECT_SOURCE_DIR}/etc/generate-uninstall.cmd\"
            mongocxx_install_manifest.txt
            ${CMAKE_INSTALL_PREFIX_WIN32}
         OUTPUT_FILE
            \"${CMAKE_CURRENT_BINARY_DIR}/${UNINSTALL_PROG}\"
      )
   ")
   install (FILES "${CMAKE_CURRENT_BINARY_DIR}/${UNINSTALL_PROG}" DESTINATION "${UNINSTALL_PROG_DIR}" PERMISSIONS ${UNINSTALL_PERMISSIONS})

   add_custom_target (uninstall
      COMMAND call "${CMAKE_CURRENT_BINARY_DIR}/${UNINSTALL_PROG}"
   )
else ()
   install (CODE "
      string(REPLACE \";\" \"\\n\" MONGOCXX_INSTALL_MANIFEST_CONTENT
         \"\${CMAKE_INSTALL_MANIFEST_FILES}\")
      file(WRITE \"mongocxx_install_manifest.txt\"
         \"\${MONGOCXX_INSTALL_MANIFEST_CONTENT}\")
      execute_process (
         COMMAND
            find \"\$ENV{DESTDIR}/${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}/bsoncxx/v_noabi\" \"\$ENV{DESTDIR}/${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}/mongocxx/v_noabi\" -type d -empty -delete
      )
      execute_process (
         COMMAND
            ${CMAKE_COMMAND} -E env
            \"${PROJECT_SOURCE_DIR}/etc/generate-uninstall.sh\"
            mongocxx_install_manifest.txt
            ${CMAKE_INSTALL_PREFIX}
         OUTPUT_FILE
            \"${CMAKE_CURRENT_BINARY_DIR}/${UNINSTALL_PROG}\"
      )
   ")
   install (FILES "${CMAKE_CURRENT_BINARY_DIR}/${UNINSTALL_PROG}" DESTINATION "${UNINSTALL_PROG_DIR}" PERMISSIONS ${UNINSTALL_PERMISSIONS})

   add_custom_target (uninstall
      COMMAND sh "${CMAKE_CURRENT_BINARY_DIR}/${UNINSTALL_PROG}"
   )
endif ()
