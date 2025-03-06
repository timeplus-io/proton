set(SRCS)

proton_add_executable(lexer lexer.cpp ${SRCS})
target_link_libraries(lexer PRIVATE clickhouse_parsers)

proton_add_executable(select_parser select_parser.cpp ${SRCS})
target_link_libraries(select_parser PRIVATE dbms)

proton_add_executable(create_parser create_parser.cpp ${SRCS})
target_link_libraries(create_parser PRIVATE dbms)
