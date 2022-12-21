add_library(wyhash INTERFACE)
target_include_directories(wyhash SYSTEM BEFORE INTERFACE "${proton_SOURCE_DIR}/contrib/wyhash")
add_library(ch_contrib::wyhash ALIAS wyhash)