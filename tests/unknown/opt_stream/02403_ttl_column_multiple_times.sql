DROP STREAM IF EXISTS ttl_table;

CREATE STREAM ttl_table
(
    EventDate Date,
    Longitude float64 TTL EventDate + to_intervalWeek(2)
)
ENGINE = MergeTree()
ORDER BY EventDate
SETTINGS vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;

SYSTEM STOP MERGES ttl_table;

INSERT INTO ttl_table VALUES(toDate('2020-10-01'), 144);

SELECT * FROM ttl_table;

SYSTEM START MERGES ttl_table;

OPTIMIZE STREAM ttl_table FINAL;

SELECT * FROM ttl_table;

OPTIMIZE STREAM ttl_table FINAL;

SELECT * FROM ttl_table;

DROP STREAM IF EXISTS ttl_table;
