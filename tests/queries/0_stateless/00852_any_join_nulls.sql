DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;
create stream table1 ( id string )  ;
create stream table2 ( parent_id string )  ;

insert into table1 values ('1');

SELECT table2.parent_id = '', isNull(table2.parent_id)
FROM table1 ANY LEFT JOIN table2 ON table1.id = table2.parent_id;

SET join_use_nulls = 1;

SELECT table2.parent_id = '', isNull(table2.parent_id)
FROM table1 ANY LEFT JOIN table2 ON table1.id = table2.parent_id;

DROP STREAM table1;
DROP STREAM table2;
