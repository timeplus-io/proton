DROP STREAM IF EXISTS t_async_insert_table_function;

CREATE STREAM t_async_insert_table_function (id uint32, s string) ENGINE = Memory;

SET async_insert = 1;

INSERT INTO function remote('127.0.0.1', current_database(), t_async_insert_table_function) values (1, 'aaa') (2, 'bbb');

SELECT * FROM t_async_insert_table_function ORDER BY id;

DROP STREAM t_async_insert_table_function;
