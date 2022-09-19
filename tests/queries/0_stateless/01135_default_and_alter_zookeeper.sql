-- Tags: zookeeper

DROP STREAM IF EXISTS default_table;

create stream default_table
(
  id uint64,
  enum_column Enum8('undefined' = 0, 'fox' = 1, 'index' = 2)
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/test_01135/default_table', '1')
ORDER BY tuple();

INSERT INTO default_table VALUES(1, 'index'), (2, 'fox');

ALTER STREAM default_table MODIFY COLUMN enum_column Enum8('undefined' = 0, 'fox' = 1, 'index' = 2) DEFAULT 'undefined';

INSERT INTO default_table (id) VALUES(3), (4);

DETACH TABLE default_table;

ATTACH TABLE default_table;

SELECT COUNT() from default_table;

ALTER STREAM default_table MODIFY COLUMN enum_column Enum8('undefined' = 0, 'fox' = 1, 'index' = 2) DEFAULT 'fox';

SHOW create stream default_table;

DROP STREAM IF EXISTS default_table;
