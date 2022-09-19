DROP STREAM IF EXISTS table_with_version;

create stream table_with_version
(
    key uint64,
    value string,
    version uint8,
    sign int8
)
ENGINE VersionedCollapsingMergeTree(sign, version)
ORDER BY key;

INSERT INTO table_with_version VALUES (1, '1', 1, -1);
INSERT INTO table_with_version VALUES (2, '2', 2, -1);

SELECT * FROM table_with_version ORDER BY key;

SHOW create stream table_with_version;

ALTER STREAM table_with_version MODIFY COLUMN version uint32;

SELECT * FROM table_with_version ORDER BY key;

SHOW create stream table_with_version;

INSERT INTO TABLE table_with_version VALUES(1, '1', 1, 1);
INSERT INTO TABLE table_with_version VALUES(1, '1', 2, 1);

SELECT * FROM table_with_version FINAL ORDER BY key;

INSERT INTO TABLE table_with_version VALUES(3, '3', 65555, 1);

SELECT * FROM table_with_version FINAL ORDER BY key;

INSERT INTO TABLE table_with_version VALUES(3, '3', 65555, -1);

SELECT * FROM table_with_version FINAL ORDER BY key;

ALTER STREAM table_with_version MODIFY COLUMN version string; --{serverError 524}
ALTER STREAM table_with_version MODIFY COLUMN version int64; --{serverError 524}
ALTER STREAM table_with_version MODIFY COLUMN version uint16; --{serverError 524}
ALTER STREAM table_with_version MODIFY COLUMN version float64; --{serverError 524}
ALTER STREAM table_with_version MODIFY COLUMN version date; --{serverError 524}
ALTER STREAM table_with_version MODIFY COLUMN version DateTime; --{serverError 524}

DROP STREAM IF EXISTS table_with_version;
