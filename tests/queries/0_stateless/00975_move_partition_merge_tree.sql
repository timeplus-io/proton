DROP STREAM IF EXISTS test_move_partition_src;
DROP STREAM IF EXISTS test_move_partition_dest;

create stream IF NOT EXISTS test_move_partition_src (
    pk uint8,
    val uint32
) Engine = MergeTree()
  PARTITION BY pk
  ORDER BY (pk, val);

create stream IF NOT EXISTS test_move_partition_dest (
    pk uint8,
    val uint32
) Engine = MergeTree()
  PARTITION BY pk
  ORDER BY (pk, val);

INSERT INTO test_move_partition_src SELECT number % 2, number FROM system.numbers LIMIT 10000000;

SELECT count() FROM test_move_partition_src;
SELECT count() FROM test_move_partition_dest;

ALTER STREAM test_move_partition_src MOVE PARTITION 1 TO TABLE test_move_partition_dest;

SELECT count() FROM test_move_partition_src;
SELECT count() FROM test_move_partition_dest;

DROP STREAM test_move_partition_src;
DROP STREAM test_move_partition_dest;
