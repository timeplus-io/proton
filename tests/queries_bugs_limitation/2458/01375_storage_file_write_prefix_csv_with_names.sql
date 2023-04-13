DROP STREAM IF EXISTS tmp_01375;
DROP STREAM IF EXISTS stream_csv_01375;

CREATE STREAM tmp_01375 (n uint32, s string) ENGINE = Memory;
CREATE STREAM stream_csv_01375 AS tmp_01375 ENGINE = File(CSVWithNames);

INSERT INTO stream_csv_01375 SELECT number as n, to_string(n) as s FROM numbers(10);
INSERT INTO stream_csv_01375 SELECT number as n, to_string(n) as s FROM numbers(10);
INSERT INTO stream_csv_01375 SELECT number as n, to_string(n) as s FROM numbers(10);

SELECT * FROM stream_csv_01375;

DROP STREAM IF EXISTS tmp_01375;
DROP STREAM IF EXISTS stream_csv_01375;
