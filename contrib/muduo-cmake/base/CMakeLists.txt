set (base_SRCS
    ${LIBRARY_DIR}/muduo/base/CountDownLatch.cc
    ${LIBRARY_DIR}/muduo/base/Timestamp.cc
    ${LIBRARY_DIR}/muduo/base/ThreadPool.cc
    ${LIBRARY_DIR}/muduo/base/AsyncLogging.cc
    ${LIBRARY_DIR}/muduo/base/Thread.cc
    ${LIBRARY_DIR}/muduo/base/ProcessInfo.cc
    ${LIBRARY_DIR}/muduo/base/Exception.cc
    ${LIBRARY_DIR}/muduo/base/Date.cc
    ${LIBRARY_DIR}/muduo/base/FileUtil.cc
    ${LIBRARY_DIR}/muduo/base/LogFile.cc
    ${LIBRARY_DIR}/muduo/base/TimeZone.cc
    ${LIBRARY_DIR}/muduo/base/CurrentThread.cc
    ${LIBRARY_DIR}/muduo/base/Logging.cc
    ${LIBRARY_DIR}/muduo/base/Condition.cc
    ${LIBRARY_DIR}/muduo/base/LogStream.cc
)

INCLUDE_DIRECTORIES(${LIBRARY_DIR}/muduo/base)

add_library(_muduo_base ${base_SRCS})

target_include_directories(_muduo_base SYSTEM PUBLIC ${LIBRARY_DIR})
target_link_libraries (_muduo_base PUBLIC boost::headers_only)

add_library (ch_contrib::muduo_base ALIAS _muduo_base)
