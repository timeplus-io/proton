proton_add_executable (delay_queue delay_queue_test.cpp)
target_link_libraries (delay_queue PRIVATE cluster_common)

proton_add_executable (stream_id_hash stream_id_hash.cpp)
target_link_libraries (stream_id_hash PRIVATE dbms)
