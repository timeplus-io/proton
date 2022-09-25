DROP STREAM IF EXISTS columns_with_multiple_streams;

SET mutations_sync = 2;

create stream columns_with_multiple_streams (
  field0 Nullable(int64) CODEC(Delta(2), LZ4),
  field1 Nullable(int64) CODEC(Delta, LZ4),
  field2 array(array(int64)) CODEC(Delta, LZ4),
  field3 tuple(uint32, array(uint64)) CODEC(T64, Default)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO columns_with_multiple_streams VALUES(1, 1, [[1]], tuple(1, [1]));

SELECT * FROM columns_with_multiple_streams;

DETACH STREAM columns_with_multiple_streams;
ATTACH STREAM columns_with_multiple_streams;

SELECT * FROM columns_with_multiple_streams;

ALTER STREAM columns_with_multiple_streams MODIFY COLUMN field1 Nullable(uint8);

INSERT INTO columns_with_multiple_streams VALUES(2, 2, [[2]], tuple(2, [2]));

SHOW create stream columns_with_multiple_streams;

SELECT * FROM columns_with_multiple_streams ORDER BY field0;

ALTER STREAM columns_with_multiple_streams MODIFY COLUMN field3 CODEC(Delta, Default);

SHOW create stream columns_with_multiple_streams;

INSERT INTO columns_with_multiple_streams VALUES(3, 3, [[3]], tuple(3, [3]));

OPTIMIZE STREAM columns_with_multiple_streams FINAL;

SELECT * FROM columns_with_multiple_streams ORDER BY field0;

DROP STREAM IF EXISTS columns_with_multiple_streams;

DROP STREAM IF EXISTS columns_with_multiple_streams_compact;

create stream columns_with_multiple_streams_compact (
  field0 Nullable(int64) CODEC(Delta(2), LZ4),
  field1 Nullable(int64) CODEC(Delta, LZ4),
  field2 array(array(int64)) CODEC(Delta, LZ4),
  field3 tuple(uint32, array(uint64)) CODEC(Delta, Default)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 100000, min_bytes_for_wide_part = 100000;

INSERT INTO columns_with_multiple_streams_compact VALUES(1, 1, [[1]], tuple(1, [1]));

SELECT * FROM columns_with_multiple_streams_compact;

DETACH STREAM columns_with_multiple_streams_compact;
ATTACH STREAM columns_with_multiple_streams_compact;

SELECT * FROM columns_with_multiple_streams_compact;

ALTER STREAM columns_with_multiple_streams_compact MODIFY COLUMN field1 Nullable(uint8);

INSERT INTO columns_with_multiple_streams_compact VALUES(2, 2, [[2]], tuple(2, [2]));

SHOW create stream columns_with_multiple_streams_compact;

SELECT * FROM columns_with_multiple_streams_compact ORDER BY field0;

ALTER STREAM columns_with_multiple_streams_compact MODIFY COLUMN field3 CODEC(Delta, Default);

SELECT * FROM columns_with_multiple_streams_compact ORDER BY field0;

SHOW create stream columns_with_multiple_streams_compact;

INSERT INTO columns_with_multiple_streams_compact VALUES(3, 3, [[3]], tuple(3, [3]));

SELECT * FROM columns_with_multiple_streams_compact ORDER BY field0;

DROP STREAM IF EXISTS columns_with_multiple_streams_compact;

DROP STREAM IF EXISTS columns_with_multiple_streams_bad_case;

-- validation still works, non-sense codecs checked
create stream columns_with_multiple_streams_bad_case (
  field0 Nullable(string) CODEC(Delta, LZ4)
)
ENGINE = MergeTree
ORDER BY tuple(); --{serverError 36}

create stream columns_with_multiple_streams_bad_case (
  field0 tuple(array(uint64), string) CODEC(T64, LZ4)
)
ENGINE = MergeTree
ORDER BY tuple(); --{serverError 431}

SET allow_suspicious_codecs = 1;

create stream columns_with_multiple_streams_bad_case (
  field0 Nullable(uint64) CODEC(Delta)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO columns_with_multiple_streams_bad_case VALUES(1), (2);

INSERT INTO columns_with_multiple_streams_bad_case VALUES(3);

OPTIMIZE STREAM columns_with_multiple_streams_bad_case FINAL;

SELECT * FROM columns_with_multiple_streams_bad_case ORDER BY field0;

DROP STREAM IF EXISTS columns_with_multiple_streams_bad_case;
