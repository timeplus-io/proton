set (CLICKHOUSE_V8_SOURCES v8.cpp)

set (CLICKHOUSE_V8_LINK
    PUBLIC
        dbms  
)

proton_program_add(v8)
