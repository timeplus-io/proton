# message(STATUS "Building CPython")
project(t)
cmake_minimum_required(VERSION 3.20)

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "CMAKE_CURRENT_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}")

message(STATUS "Building CPython")

set(confcommand "CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./configure")
string(APPEND confcommand " --prefix=${CMAKE_CURRENT_BINARY_DIR}/")

if (CMAKE_BUILD_TYPE STREQUAL "debug")
    string(APPEND confcommand " --with-pydebug=yes")
    string(APPEND confcommand " --with-trace-refs=yes")
    string(APPEND confcommand " --with-assertions=yes")
    string(APPEND confcommand " --with-strict-overflow=yes")
endif()

if (DISABLE_GIL STREQUAL "yes")
    message(STATUS "Building CPython with GIL disabled(experimental)")
    string(APPEND confcommand " --disable-gil=yes")
endif()

if (SANITIZE STREQUAL "addresses")
    string(APPEND confcommand " --with-sanitizers=yes")
endif()


message(STATUS "using command ${confcommand} to configure CPython")

add_custom_command(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.10.a
                   COMMAND ./configure  "LDFLAGS=-static -static-libgcc" "CPPFLAGS=-static"
                   COMMAND make -j 20
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cpython
                   COMMENT "Building library")
add_custom_target(libpython ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.10.a)

# if(TARGET libpython)
#     message(STATUS "libpython target exists")
# else()
#     message(STATUS "libpython target does not exist")
# endif()

# method 1: import the library as an imported target
# add_library(python_ STATIC IMPORTED)
# set_target_properties(python_ PROPERTIES IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.10.a)
# set_target_properties(python_ PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/cpython/Include/)
# add_library(ch_contrib::python ALIAS python_)



# add_library(test1 dummy.cpp)
# target_link_libraries(test1 ch_contrib::python)
# method 2: add the library as a static library
# add_library(python_ STATIC ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.10.a)

add_library(_python STATIC IMPORTED GLOBAL)
set_target_properties(_python PROPERTIES IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.10.a)
target_include_directories(_python SYSTEM BEFORE INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/cpython/Include)
target_include_directories(_python SYSTEM BEFORE INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/cpython)

add_library(ch_contrib::python ALIAS _python)

message(STATUS "Building CPython: Done")
