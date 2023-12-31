CREATE STREAM testing
(
    a string,
    b string,
    c int32,
    d int32,
    e int32,
    PROJECTION proj_1
    (
        SELECT c ORDER BY d
    ),
    PROJECTION proj_2
    (
        SELECT c ORDER BY e, d
    )
)
ENGINE = MergeTree() PRIMARY KEY (a) SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO testing SELECT number, number, number, number, number%2 FROM numbers(5);

-- { echoOn }

OPTIMIZE STREAM testing FINAL;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- update all colums used by proj_1
ALTER STREAM testing UPDATE c = c+1, d = d+2 WHERE True SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = current_database() AND stream = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;


-- update only one column
ALTER STREAM testing UPDATE d = d-1 WHERE True SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = current_database() AND stream = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;


-- update only another one column
ALTER STREAM testing UPDATE c = c-1 WHERE True SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = current_database() AND stream = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- { echoOff }

DROP STREAM testing;
