set(ABSL_ROOT_DIR "${proton_SOURCE_DIR}/contrib/abseil-cpp")
if(NOT EXISTS "${ABSL_ROOT_DIR}/CMakeLists.txt")
  message(FATAL_ERROR " submodule third_party/abseil-cpp is missing. To fix try run: \n git submodule update --init --recursive")
endif()
set(BUILD_TESTING OFF)
set(ABSL_PROPAGATE_CXX_STD ON)
add_subdirectory("${ABSL_ROOT_DIR}" "${proton_BINARY_DIR}/contrib/abseil-cpp")

add_library(_abseil_swiss_tables INTERFACE)

target_link_libraries(_abseil_swiss_tables INTERFACE
  absl::flat_hash_map
  absl::flat_hash_set
  absl::btree
)

get_target_property(FLAT_HASH_MAP_INCLUDE_DIR absl::flat_hash_map INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (_abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_HASH_MAP_INCLUDE_DIR})

get_target_property(FLAT_HASH_SET_INCLUDE_DIR absl::flat_hash_set INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (_abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_HASH_SET_INCLUDE_DIR})

get_target_property(FLAT_BTREE_MAP_INCLUDE_DIR absl::btree INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (_abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_BTREE_MAP_INCLUDE_DIR})

add_library(ch_contrib::abseil_swiss_tables ALIAS _abseil_swiss_tables)
