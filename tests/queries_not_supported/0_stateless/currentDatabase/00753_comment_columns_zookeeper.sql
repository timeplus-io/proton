-- Tags: zookeeper

DROP STREAM IF EXISTS check_comments;

create stream check_comments
  (
    column_name1 uint8 DEFAULT 1 COMMENT 'comment',
    column_name2 uint8 COMMENT 'non default comment'
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00753/comments', 'r1')
    ORDER BY column_name1;

SHOW CREATE check_comments;
DESC check_comments;

ALTER STREAM check_comments COMMENT COLUMN column_name1 'another comment';

SHOW CREATE check_comments;
DESC check_comments;

SELECT * FROM system.columns WHERE table = 'check.comments' and database = currentDatabase();

DROP STREAM check_comments;
