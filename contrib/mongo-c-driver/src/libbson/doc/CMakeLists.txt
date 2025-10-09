include (SphinxBuild)

if (ENABLE_HTML_DOCS)
   sphinx_build_html (bson-html libbson)
endif ()

if (ENABLE_MAN_PAGES)
   sphinx_build_man (bson-man)
endif ()
