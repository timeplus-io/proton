include (SphinxBuild)

if (ENABLE_HTML_DOCS)
   sphinx_build_html (mongoc-html mongo-c-driver)
endif ()

if (ENABLE_MAN_PAGES)
   sphinx_build_man (mongoc-man)
endif ()
