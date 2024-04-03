project(numpy)
cmake_minimum_required(VERSION 3.20)


message(STATUS "Building numpy library")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build-install/usr/lib/python3/dist-packages/numpy/_core/include)

add_custom_command(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy_core/_multiarray_umath.cpython-310-x86_64-linux-gnu.so
                   COMMAND spin build
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/numpy
                   COMMENT "Building numpy library")

add_custom_target(libnumpy ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/_multiarray_umath.cpython-310-x86_64-linux-gnu.so)


add_library(_numpy SHARED IMPORTED GLOBAL)

set_target_properties(_numpy PROPERTIES
  IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/_multiarray_umath.cpython-310-x86_64-linux-gnu.so
)

target_include_directories(_numpy SYSTEM BEFORE INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build-install/usr/lib/python3/dist-packages/numpy/_core/include)

add_library(ch_contrib::numpy ALIAS _numpy)

message(STATUS "Building numpy library done")