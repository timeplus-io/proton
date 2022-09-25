DROP STREAM IF EXISTS table_with_column_ttl;
create stream table_with_column_ttl
(
    EventTime datetime,
    UserID uint64,
    Age uint8 TTL EventTime + INTERVAL 3 MONTH
)
ENGINE MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0; -- column TTL doesn't work for compact parts

INSERT INTO table_with_column_ttl VALUES (now(), 1, 32);

INSERT INTO table_with_column_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 45);

OPTIMIZE STREAM table_with_column_ttl FINAL;

SELECT UserID, Age FROM table_with_column_ttl ORDER BY UserID;

ALTER STREAM table_with_column_ttl MODIFY COLUMN Age REMOVE TTL;

SHOW create stream table_with_column_ttl;

INSERT INTO table_with_column_ttl VALUES (now() - INTERVAL 10 MONTH, 3, 27);

OPTIMIZE STREAM table_with_column_ttl FINAL;

SELECT UserID, Age FROM table_with_column_ttl ORDER BY UserID;

DROP STREAM table_with_column_ttl;
