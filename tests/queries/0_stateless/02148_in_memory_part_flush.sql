DROP STREAM IF EXISTS mem_part_flush;

create stream mem_part_flush
(
`key` uint32,
`ts` DateTime,
`db_time` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (key, ts)
SETTINGS min_rows_for_compact_part = 1000000, min_bytes_for_compact_part = 200000000, in_memory_parts_enable_wal = 0;

INSERT INTO mem_part_flush(key, ts) SELECT number % 1000, now() + int_div(number,1000) FROM numbers(500);

SELECT 'before DETACH TABLE';
SELECT count(*) FROM mem_part_flush;

DETACH TABLE mem_part_flush;

ATTACH TABLE mem_part_flush;

SELECT 'after DETACH TABLE';
SELECT count(*) FROM mem_part_flush;


DROP STREAM mem_part_flush;
