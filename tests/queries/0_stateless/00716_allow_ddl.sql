-- Tags: no-parallel

SET send_logs_level = 'fatal';
SET allow_ddl = 0;

CREATE DATABASE some_db; -- { serverError 392 } 
create stream some_table(a int32) ; -- { serverError 392}
ALTER STREAM some_table DELETE WHERE 1; -- { serverError 392}
RENAME TABLE some_table TO some_table1; -- { serverError 392}
SET allow_ddl = 1; -- { serverError 392}
