proton_add_executable (remove_symlink_directory remove_symlink_directory.cpp)
target_link_libraries (remove_symlink_directory PRIVATE dbms)

proton_add_executable (merge_selector merge_selector.cpp)
target_link_libraries (merge_selector PRIVATE dbms)

proton_add_executable (merge_selector2 merge_selector2.cpp)
target_link_libraries (merge_selector2 PRIVATE dbms)
