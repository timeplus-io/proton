DROP STREAM IF EXISTS test;
create stream test (`key` uint32, `arr` ALIAS [1, 2], `xx` MATERIALIZED arr[1]) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple(); 
DROP STREAM test;

create stream test (`key` uint32, `arr` array(uint32) ALIAS [1, 2], `xx` MATERIALIZED arr[1]) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();
DROP STREAM test;

create stream test (`key` uint32, `arr` array(uint32) ALIAS [1, 2], `xx` uint32 MATERIALIZED arr[1]) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();
DROP STREAM test;

create stream test (`key` uint32, `arr` ALIAS [1, 2]) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();
ALTER STREAM test ADD COLUMN `xx` uint32 MATERIALIZED arr[1];
DROP STREAM test;
