option (ENABLE_MUDUO "Enable muduo" ${ENABLE_LIBRARIES})

if (NOT ENABLE_MUDUO)
    message(STATUS "Not using muduo")
    return()
endif()

set (LIBRARY_DIR ${proton_SOURCE_DIR}/contrib/muduo)

add_subdirectory(base)
add_subdirectory(net)
