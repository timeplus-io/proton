DROP STREAM IF EXISTS test_table;
DROP STREAM IF EXISTS test_table_sharded;

create stream
  test_table_sharded(
    date date,
    text string,
    hash uint64
  )
engine=MergeTree(date, (hash, date), 8192);

create stream test_table as test_table_sharded
engine=Distributed(test_cluster_two_shards, currentDatabase(), test_table_sharded, hash);

SET distributed_product_mode = 'local';
SET insert_distributed_sync = 1;

INSERT INTO test_table VALUES ('2020-04-20', 'Hello', 123);

SELECT
  text,
  uniqExactIf(hash, hash IN (
    SELECT DISTINCT
      hash
    FROM test_table AS t1
  )) as counter
FROM test_table AS t2
GROUP BY text
ORDER BY counter, text;

DROP STREAM test_table;
DROP STREAM test_table_sharded;
