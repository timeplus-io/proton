add_subdirectory(MergeTree)
add_subdirectory(System)
add_subdirectory(Stream)
add_subdirectory(Random)
add_subdirectory(Proxy)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
