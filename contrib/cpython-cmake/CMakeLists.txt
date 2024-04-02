# message(STATUS "Building CPython")

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "CMAKE_CURRENT_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}")

message(STATUS "Building CPython")

add_custom_command(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.13.a
                   COMMAND ./configure
                   COMMAND make
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cpython
                   COMMENT "Building library")
add_custom_target(libpython ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.13.a)

if(TARGET libpython)
    message(STATUS "libpython target exists")
else()
    message(STATUS "libpython target does not exist")
endif()

# method 1: import the library as an imported target
# add_library(python_ STATIC IMPORTED)
# set_target_properties(python_ PROPERTIES IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.13.a)
# set_target_properties(python_ PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/cpython/Include/)
# add_library(ch_contrib::python ALIAS python_)



# add_library(test1 dummy.cpp)
# target_link_libraries(test1 ch_contrib::python)
# method 2: add the library as a static library
add_library(python_ STATIC ${CMAKE_CURRENT_SOURCE_DIR}/cpython/libpython3.13.a)
add_library(ch_contrib::python ALIAS python_)
target_include_directories(python_ SYSTEM BEFORE PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpython/Include)
message(STATUS "Building CPython: Done")

# add_library(ch_contrib::python ALIAS libpython)
# target_include_directories(libpython SYSTEM BEFORE PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpython/Include)

