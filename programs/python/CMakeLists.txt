set (CLICKHOUSE_PYTHON_SOURCES python.cpp)
set (CLICKHOUSE_PYTHON_LINK PRIVATE dbms)

proton_program_add(python)
