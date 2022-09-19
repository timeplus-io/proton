DROP TEMPORARY STREAM IF EXISTS table_to_drop;
DROP STREAM IF EXISTS table_to_drop;

create stream table_to_drop(x int8) ENGINE=Log;
CREATE TEMPORARY STREAM table_to_drop(x int8);
DROP TEMPORARY TABLE table_to_drop;
DROP TEMPORARY TABLE table_to_drop; -- { serverError 60 }
DROP STREAM table_to_drop;
DROP STREAM table_to_drop; -- { serverError 60 }

create stream table_to_drop(x int8) ENGINE=Log;
CREATE TEMPORARY STREAM table_to_drop(x int8);
DROP STREAM table_to_drop;
DROP STREAM table_to_drop;
DROP STREAM table_to_drop; -- { serverError 60 }
