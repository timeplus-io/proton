DROP STREAM IF EXISTS tdm__fuzz_23;
CREATE STREAM tdm__fuzz_23 (`x` uint256) ENGINE = MergeTree ORDER BY x SETTINGS write_final_mark = 0;
INSERT INTO tdm__fuzz_23 FORMAT Values (1);
SELECT count(x) FROM tdm__fuzz_23 WHERE to_date(x) < to_date(now(), 'Asia/Istanbul') SETTINGS max_rows_to_read = 1;
DROP STREAM tdm__fuzz_23;
