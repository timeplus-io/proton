-- Tags: long, zookeeper

DROP STREAM IF EXISTS modify_sample;

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 10;

create stream modify_sample (d date DEFAULT '2000-01-01', x uint8) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO modify_sample (x) SELECT to_uint8(number) AS x FROM system.numbers LIMIT 256;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM modify_sample SAMPLE 0.1; -- { serverError 141 }

ALTER STREAM modify_sample MODIFY SAMPLE BY x;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM modify_sample SAMPLE 0.1;

create stream modify_sample_replicated (d date DEFAULT '2000-01-01', x uint8, y uint64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01430', 'modify_sample') PARTITION BY d ORDER BY (x, y);

INSERT INTO modify_sample_replicated (x, y) SELECT to_uint8(number) AS x, to_uint64(number) as y FROM system.numbers LIMIT 256;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM modify_sample_replicated SAMPLE 0.1; -- { serverError 141 }

ALTER STREAM modify_sample_replicated MODIFY SAMPLE BY x;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM modify_sample_replicated SAMPLE 0.1;

DETACH TABLE modify_sample_replicated;
ATTACH TABLE modify_sample_replicated;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM modify_sample_replicated SAMPLE 0.1;

ALTER STREAM modify_sample_replicated MODIFY SAMPLE BY d;  -- { serverError 36 }
ALTER STREAM modify_sample_replicated MODIFY SAMPLE BY y;

SELECT count(), min(y), max(y), sum(y), uniqExact(y) FROM modify_sample_replicated SAMPLE 0.1;

DETACH TABLE modify_sample_replicated;
ATTACH TABLE modify_sample_replicated;

SELECT count(), min(y), max(y), sum(y), uniqExact(y) FROM modify_sample_replicated SAMPLE 0.1;

create stream modify_sample_old (d date DEFAULT '2000-01-01', x uint8, y uint64) ENGINE = MergeTree(d, (x, y), 8192);

ALTER STREAM modify_sample_old MODIFY SAMPLE BY x; -- { serverError 36 }

DROP STREAM modify_sample;

DROP STREAM modify_sample_replicated;

DROP STREAM modify_sample_old;
