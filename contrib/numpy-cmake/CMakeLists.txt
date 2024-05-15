project(numpy)
cmake_minimum_required(VERSION 3.20)


message(STATUS "Building numpy library")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build-install/usr/lib/python3/dist-packages/numpy/_core/include)


add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/libnumpy.so
    COMMAND spin build
    # COMMAND bash -c "lib_path=$$(find ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core -name '_multiarray_umath.cpython*') && mv $$lib_path ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/libnumpy.so"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/numpy
    COMMENT "Building and renaming numpy library"
)

add_custom_target(libnumpy ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/libnumpy.so)


add_library(_numpy SHARED IMPORTED GLOBAL)

set_target_properties(_numpy PROPERTIES
  IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build/numpy/_core/libnumpy.so
)
get_target_property(_numpy_location _numpy IMPORTED_LOCATION)
message(STATUS "Imported location of _numpy: ${_numpy_location}")

target_include_directories(_numpy SYSTEM BEFORE INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/numpy/build-install/usr/lib/python3/dist-packages/numpy/_core/include)

add_library(ch_contrib::numpy ALIAS _numpy)

message(STATUS "Building numpy library done")