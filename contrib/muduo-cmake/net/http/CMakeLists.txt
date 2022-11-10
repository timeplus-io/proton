set(http_SRCS
  ${LIBRARY_DIR}/muduo/net/http/HttpServer.cc
  ${LIBRARY_DIR}/muduo/net/http/HttpResponse.cc
  ${LIBRARY_DIR}/muduo/net/http/HttpContext.cc
  )

INCLUDE_DIRECTORIES(${LIBRARY_DIR}/muduo/net/http)

add_library(_muduo_http ${http_SRCS})
target_include_directories(_muduo_http PUBLIC ${LIBRARY_DIR})
target_link_libraries(_muduo_http _muduo_net)

add_library (ch_contrib::muduo_http ALIAS _muduo_http)