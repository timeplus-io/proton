DROP STREAM IF EXISTS compress_table;

create stream compress_table
(
  key uint64,
  value1 string CODEC(Default),
  value2 uint64 CODEC(Delta, Default),
  value3 string CODEC(ZSTD(10))
)
ENGINE = MergeTree()
ORDER BY key;

INSERT INTO compress_table VALUES(1, '1', '1', '1');

SELECT * FROM compress_table;

ALTER STREAM compress_table MODIFY COLUMN value3 CODEC(Default);

INSERT INTO compress_table VALUES(2, '2', '2', '2');

SELECT * FROM compress_table ORDER BY key;

DESCRIBE TABLE compress_table;

SHOW create stream compress_table;

ALTER STREAM compress_table MODIFY COLUMN value2 CODEC(Default(5)); --{serverError 36}

DROP STREAM IF EXISTS compress_table;
