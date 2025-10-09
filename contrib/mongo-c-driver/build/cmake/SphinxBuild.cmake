# function (sphinx_build_html)
#
# Parameters:
#
# target_name - name of the generated custom target (e.g., bson-html)
# doc_dir - name of the destination directory for HTML documentation; is used
# to construct a target path: ${CMAKE_INSTALL_PREFIX}/share/doc/${doc_dir}/html
#
# Description:
#
# Process the list of .rst files in the current source directory and:
# - build up a list of target/output .html files
# - create the custom Sphinx command that produces the HTML output
# - add install rules for:
#   + source static content (e.g., images)
#   + each output file
#   + additional Sphinx-generated content
# - create the custom target that depends on the output files and calls sphinx
function (sphinx_build_html target_name doc_dir)
   include (ProcessorCount)
   ProcessorCount (NPROCS)

   set (SPHINX_HTML_DIR "${CMAKE_CURRENT_BINARY_DIR}/html")
   set (doctrees_dir "${SPHINX_HTML_DIR}.doctrees")

   file (GLOB_RECURSE doc_rsts RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} CONFIGURE_DEPENDS *.rst)

   # Select a builder: The Sphinx `dirhtml` builder will result in "prettier" URLs,
   # as each page is an `index.html` within a directory. This relies on "autoindex"-style
   # static file URL resolution in the HTTP server, which is extremely common. For
   # EVG, however, we host build results on a server that does not support this,
   # so instead use the traditional HTML builder so that the built documentation
   # artifact can be easily viewed in a web browser
   set(is_evg_docs_build "$ENV{EVG_DOCS_BUILD}")
   if(NOT is_evg_docs_build)
      set (builder dirhtml)
      # We have an extension in place that generates stub page redirects for
      # old HTML file URLs that now point to the auto-index URL for the
      # corresponding page. As such, every .rst builds two corresponding .html
      # files:
      list (TRANSFORM doc_rsts
            REPLACE "^(.+)\\.rst$" "html/\\1.html;html/\\1/index.html"
            OUTPUT_VARIABLE doc_htmls)
   else()
      set (builder html)
      # We use the regular html builder in this case, which generates exactly
      # one HTML page for each input rST file
      list (TRANSFORM doc_rsts
            REPLACE "^(.+)\\.rst$" "html/\\1.html"
            OUTPUT_VARIABLE doc_htmls)
   endif()

   # Set PYTHONDONTWRITEBYTECODE to prevent .pyc clutter in the source directory
   add_custom_command (OUTPUT ${doc_htmls}
      ${SPHINX_HTML_DIR}/.nojekyll ${SPHINX_HTML_DIR}/objects.inv
      COMMAND
      ${CMAKE_COMMAND} -E env
         "PYTHONDONTWRITEBYTECODE=1"
      ${SPHINX_EXECUTABLE}
         -qnW -b "${builder}"
         -j "${NPROCS}"
         -c "${CMAKE_CURRENT_SOURCE_DIR}"
         -d "${doctrees_dir}"
         "${CMAKE_CURRENT_SOURCE_DIR}"
         "${SPHINX_HTML_DIR}"
      DEPENDS
      ${doc_rsts}
      COMMENT
      "Building HTML documentation with Sphinx"
   )

   # Install all HTML files
   install (DIRECTORY "${SPHINX_HTML_DIR}/"
            DESTINATION "${CMAKE_INSTALL_DOCDIR}/${doc_dir}/html"
            FILES_MATCHING PATTERN "*.html")

   # Ensure additional Sphinx-generated content gets installed
   install (FILES
      ${SPHINX_HTML_DIR}/search.html
      ${SPHINX_HTML_DIR}/objects.inv
      ${SPHINX_HTML_DIR}/searchindex.js
      DESTINATION
      ${CMAKE_INSTALL_DOCDIR}/${doc_dir}/html
   )
   install (DIRECTORY
      ${SPHINX_HTML_DIR}/_static
      DESTINATION
      ${CMAKE_INSTALL_DOCDIR}/${doc_dir}/html
   )

   add_custom_target (${target_name} DEPENDS ${doc_htmls})
endfunction ()

# function (sphinx_build_man)
#
# Parameters:
#
# target_name - name of the generated custom target (e.g., mongoc-man)
#
# Description:
#
# Process the list of .rst files in the current source directory and:
# - build up a list of target/output .3 files (i.e., man pages)
# - create the custom Sphinx command that produces the man page output
# - add install rules for each output file
# - create the custom target that depends on the output files and calls sphinx
#
function (sphinx_build_man target_name)
   include (ProcessorCount)
   ProcessorCount (NPROCS)

   set (SPHINX_MAN_DIR "${CMAKE_CURRENT_BINARY_DIR}/man")
   set (doctrees_dir "${SPHINX_MAN_DIR}.doctrees")

   file (GLOB_RECURSE doc_rsts RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} CONFIGURE_DEPENDS *.rst)

   set (doc_mans)
   foreach (rst IN LISTS doc_rsts)
      # Only those with the :man_page: tag at the beginning build man pages
      file (READ "${rst}" rst_head LIMIT 256)
      string (FIND "${rst_head}" ":man_page: " man_tag_pos)
      if (man_tag_pos GREATER_EQUAL "0")
         list (APPEND man_doc_rsts "${rst}")
         string (REGEX REPLACE
            "^.*:man_page: +([a-z0-9_]+).*$" "man\/\\1.3"
            man
            ${rst_head}
         )
         list (APPEND doc_mans ${man})
      endif ()
   endforeach ()

   # Set PYTHONDONTWRITEBYTECODE to prevent .pyc clutter in the source directory
   add_custom_command (OUTPUT ${doc_mans}
      COMMAND
      ${CMAKE_COMMAND} -E env
         "PYTHONDONTWRITEBYTECODE=1"
      ${SPHINX_EXECUTABLE}
         -qW -b man
         -c "${CMAKE_CURRENT_SOURCE_DIR}"
         -d "${doctrees_dir}"
         "${CMAKE_CURRENT_SOURCE_DIR}"
         "${SPHINX_MAN_DIR}"
      DEPENDS
      ${doc_rsts}
      COMMENT
      "Building manual pages with Sphinx"
   )

   foreach (man IN LISTS doc_mans)
      install (FILES
         ${CMAKE_CURRENT_BINARY_DIR}/${man}
         DESTINATION
         ${CMAKE_INSTALL_MANDIR}/man3
      )
   endforeach ()

   add_custom_target (${target_name} DEPENDS ${doc_mans})
endfunction ()
