DROP STREAM IF EXISTS stream1;
DROP STREAM IF EXISTS stream2;
CREATE STREAM stream1 ( id string ) ENGINE = Log;
CREATE STREAM stream2 ( parent_id string ) ENGINE = Log;

insert into stream1 values ('1');

SELECT stream2.parent_id = '', is_null(table2.parent_id)
FROM stream1 ANY LEFT JOIN stream2 ON stream1.id = stream2.parent_id;

SET join_use_nulls = 1;

SELECT stream2.parent_id = '', is_null(table2.parent_id)
FROM stream1 ANY LEFT JOIN stream2 ON stream1.id = stream2.parent_id;

DROP STREAM stream1;
DROP STREAM stream2;
