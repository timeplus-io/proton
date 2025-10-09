function (SET_LOCAL_DIST output)
   set (dist_files "")
   foreach (file ${ARGN})
      file (RELATIVE_PATH
         relative
         ${CMAKE_SOURCE_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/${file}
      )
      list (APPEND dist_files ${relative})
   endforeach ()
   set (${output} ${dist_files} PARENT_SCOPE)
endfunction ()

macro (SET_DIST_LIST output)
   set_local_dist (${output}_TMP ${ARGN})
   set (${output} ${${output}_TMP} PARENT_SCOPE)
endmacro ()

function (EXECUTE_PROCESS_AND_CHECK_RESULT)
   cmake_parse_arguments (VARS
      ""
      "WORKING_DIRECTORY;ERROR_MSG"
      "COMMAND"
      ${ARGN}
   )
   execute_process (COMMAND
      ${VARS_COMMAND}
      WORKING_DIRECTORY ${VARS_WORKING_DIRECTORY}
      RESULT_VARIABLE RESULT
   )
   if (NOT "${RESULT}" STREQUAL "0")
      message (FATAL_ERROR ${VARS_ERROR_MSG})
   endif ()
endfunction ()

# Add a file or list of files to the distribution manifest to be included in the
# archive. The parameter _is_built distinguishes between files that appear in
# the source tree and those which are generated and then appear in the binary,
# or built, tree.
function (EXTRA_DIST_INTERNAL _is_built)
   if (_is_built)
      set (DIST_BUILD_SOURCE_DIR ${CMAKE_BINARY_DIR})
      set (DIST_CURRENT_BUILD_SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR})
   else ()
      set (DIST_BUILD_SOURCE_DIR ${CMAKE_SOURCE_DIR})
      set (DIST_CURRENT_BUILD_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
   endif ()

   set (local_generated ${dist_generated})
   foreach (gen_file ${ARGN})
      file (RELATIVE_PATH
         rel_gen
         ${DIST_BUILD_SOURCE_DIR}
         ${DIST_CURRENT_BUILD_SOURCE_DIR}/${gen_file}
      )
      list (APPEND local_generated ${rel_gen})
   endforeach ()
   set (dist_generated
      ${local_generated}
      CACHE
      INTERNAL
      "generated files that will be included in the distribution tarball"
   )
endfunction ()

function (EXTRA_DIST_SOURCE)
   extra_dist_internal (NO ${ARGN})
endfunction ()

function (EXTRA_DIST_GENERATED)
   extra_dist_internal (YES ${ARGN})
endfunction ()
