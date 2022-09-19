DROP STREAM IF EXISTS offset_without_limit;

create stream offset_without_limit (
    value uint32
) Engine = MergeTree()
  PRIMARY KEY value
  ORDER BY value;

INSERT INTO offset_without_limit SELECT * FROM system.numbers LIMIT 50;

SELECT value FROM offset_without_limit ORDER BY value OFFSET 5;

DROP STREAM offset_without_limit;
