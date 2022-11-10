set(inspect_SRCS
  ${LIBRARY_DIR}/muduo/net/inspect/Inspector.cc
  ${LIBRARY_DIR}/muduo/net/inspect/PerformanceInspector.cc
  ${LIBRARY_DIR}/muduo/net/inspect/ProcessInspector.cc
  ${LIBRARY_DIR}/muduo/net/inspect/SystemInspector.cc
  )

INCLUDE_DIRECTORIES(${LIBRARY_DIR}/muduo/net/inspect)

add_library(_muduo_inspect ${inspect_SRCS})
target_link_libraries(_muduo_inspect _muduo_http)
target_include_directories(_muduo_inspect PUBLIC ${LIBRARY_DIR})

if(TCMALLOC_INCLUDE_DIR AND TCMALLOC_LIBRARY)
  set_target_properties(_muduo_inspect PROPERTIES COMPILE_FLAGS "-DHAVE_TCMALLOC")
  target_link_libraries(_muduo_inspect tcmalloc_and_profiler)
endif()

add_library (ch_contrib::muduo_inspect ALIAS _muduo_inspect)
