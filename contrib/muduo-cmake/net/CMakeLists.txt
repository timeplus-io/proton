include(CheckFunctionExists)

check_function_exists(accept4 HAVE_ACCEPT4)
if(NOT HAVE_ACCEPT4)
  set_source_files_properties(${LIBRARY_DIR}/muduo/net/SocketsOps.cc PROPERTIES COMPILE_FLAGS "-DNO_ACCEPT4")
endif()

set (net_SRCS
    ${LIBRARY_DIR}/muduo/net/TcpConnection.cc
    ${LIBRARY_DIR}/muduo/net/InetAddress.cc
    ${LIBRARY_DIR}/muduo/net/TcpServer.cc
    ${LIBRARY_DIR}/muduo/net/EventLoopThread.cc
    ${LIBRARY_DIR}/muduo/net/TimerQueue.cc
    ${LIBRARY_DIR}/muduo/net/Poller.cc
    ${LIBRARY_DIR}/muduo/net/TcpClient.cc
    ${LIBRARY_DIR}/muduo/net/Acceptor.cc
    ${LIBRARY_DIR}/muduo/net/Timer.cc
    ${LIBRARY_DIR}/muduo/net/Channel.cc
    ${LIBRARY_DIR}/muduo/net/poller/DefaultPoller.cc
    ${LIBRARY_DIR}/muduo/net/poller/EPollPoller.cc
    ${LIBRARY_DIR}/muduo/net/poller/PollPoller.cc
    ${LIBRARY_DIR}/muduo/net/Buffer.cc
    ${LIBRARY_DIR}/muduo/net/Connector.cc
    ${LIBRARY_DIR}/muduo/net/SocketsOps.cc
    ${LIBRARY_DIR}/muduo/net/EventLoopThreadPool.cc
    ${LIBRARY_DIR}/muduo/net/EventLoop.cc
    ${LIBRARY_DIR}/muduo/net/Socket.cc
)

add_subdirectory(http)
add_subdirectory(inspect)

INCLUDE_DIRECTORIES(${LIBRARY_DIR}/muduo/net)

add_library(_muduo_net ${net_SRCS})

target_link_libraries(_muduo_net _muduo_base)
target_include_directories(_muduo_net SYSTEM PUBLIC ${LIBRARY_DIR})
add_library (ch_contrib::muduo_net ALIAS _muduo_net)

