add_subdirectory(MergeTree)
add_subdirectory(System)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

add_subdirectory(Streaming)
