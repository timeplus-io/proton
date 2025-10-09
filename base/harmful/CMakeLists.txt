add_library(harmful harmful.c)
install(TARGETS harmful EXPORT global ARCHIVE DESTINATION lib)

if (ENABLE_PYTHON_UDF)
    target_compile_definitions(harmful PRIVATE USE_PYTHON_UDF)
endif()
