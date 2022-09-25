DROP STREAM IF EXISTS t_ttl_modify_column;

create stream t_ttl_modify_column
(
    InsertionDateTime datetime,
    TTLDays int32 DEFAULT CAST(365, 'int32')
)
ENGINE = MergeTree
ORDER BY tuple()
TTL InsertionDateTime + toIntervalDay(TTLDays)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_modify_column VALUES (now(), 23);

SET mutations_sync = 2;

ALTER STREAM t_ttl_modify_column modify column TTLDays int16 DEFAULT CAST(365, 'int16');

INSERT INTO t_ttl_modify_column VALUES (now(), 23);

SELECT sum(rows), groupUniqArray(type) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_modify_column' AND column = 'TTLDays' AND active;

DROP STREAM IF EXISTS t_ttl_modify_column;

create stream t_ttl_modify_column (InsertionDateTime datetime)
ENGINE = MergeTree
ORDER BY tuple()
TTL InsertionDateTime + INTERVAL 3 DAY
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_modify_column VALUES (now());

ALTER STREAM t_ttl_modify_column MODIFY COLUMN InsertionDateTime date;

INSERT INTO t_ttl_modify_column VALUES (now());

SELECT sum(rows), groupUniqArray(type) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_modify_column' AND column = 'InsertionDateTime' AND active;

ALTER STREAM t_ttl_modify_column MODIFY COLUMN InsertionDateTime string; -- { serverError 43 }

DROP STREAM IF EXISTS t_ttl_modify_column;
