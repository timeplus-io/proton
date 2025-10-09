set(PYTHON_SOURCES
    ${CPYTHON_SOURCE_DIR}/Programs/python.c
)

add_executable(python ${PYTHON_SOURCES})
target_compile_definitions(python PRIVATE Py_BUILD_CORE)

target_link_libraries(python PRIVATE libpython-static)
set_target_properties(python PROPERTIES
    COMPILE_DEFINITIONS Py_NO_ENABLE_SHARED
    ENABLE_EXPORTS ON
)
