DROP STREAM IF EXISTS nullable_key;
DROP STREAM IF EXISTS nullable_key_without_final_mark;
DROP STREAM IF EXISTS nullable_minmax_index;

SET max_threads = 1;

create stream nullable_key (k Nullable(int), v int) ENGINE MergeTree ORDER BY k SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO nullable_key SELECT number * 2, number * 3 FROM numbers(10);
INSERT INTO nullable_key SELECT NULL, -number FROM numbers(3);

SELECT * FROM nullable_key ORDER BY k;

SET force_primary_key = 1;
SET max_rows_to_read = 3;
SELECT * FROM nullable_key WHERE k IS NULL;
SET max_rows_to_read = 10;
SELECT * FROM nullable_key WHERE k IS NOT NULL;
SET max_rows_to_read = 5;
SELECT * FROM nullable_key WHERE k > 10;
SELECT * FROM nullable_key WHERE k < 10;

OPTIMIZE STREAM nullable_key FINAL;

SET max_rows_to_read = 4; -- one additional left mark needs to be read
SELECT * FROM nullable_key WHERE k IS NULL;
SET max_rows_to_read = 10;
SELECT * FROM nullable_key WHERE k IS NOT NULL;

-- Nullable in set and with transform_null_in = 1
SET max_rows_to_read = 3;
SELECT * FROM nullable_key WHERE k IN (10, 20) SETTINGS transform_null_in = 1;
SET max_rows_to_read = 5;
SELECT * FROM nullable_key WHERE k IN (3, NULL) SETTINGS transform_null_in = 1;

create stream nullable_key_without_final_mark (s Nullable(string)) ENGINE MergeTree ORDER BY s SETTINGS allow_nullable_key = 1, write_final_mark = 0;
INSERT INTO nullable_key_without_final_mark VALUES ('123'), (NULL);
SET max_rows_to_read = 0;
SELECT * FROM nullable_key_without_final_mark WHERE s IS NULL;
SELECT * FROM nullable_key_without_final_mark WHERE s IS NOT NULL;

create stream nullable_minmax_index (k int, v Nullable(int), INDEX v_minmax v TYPE minmax GRANULARITY 4) ENGINE MergeTree ORDER BY k SETTINGS index_granularity = 1;

INSERT INTO nullable_minmax_index VALUES (1, 3), (2, 7), (3, 4), (2, NULL); -- [3, +Inf]
INSERT INTO nullable_minmax_index VALUES (1, 1), (2, 2), (3, 2), (2, 1); -- [1, 2]
INSERT INTO nullable_minmax_index VALUES (2, NULL), (3, NULL); -- [+Inf, +Inf]

SET force_primary_key = 0;
SELECT * FROM nullable_minmax_index ORDER BY k;
SET max_rows_to_read = 6;
SELECT * FROM nullable_minmax_index WHERE v IS NULL;
SET max_rows_to_read = 8;
SELECT * FROM nullable_minmax_index WHERE v IS NOT NULL;
SET max_rows_to_read = 6;
SELECT * FROM nullable_minmax_index WHERE v > 2;
SET max_rows_to_read = 4;
SELECT * FROM nullable_minmax_index WHERE v <= 2;

DROP STREAM nullable_key;
DROP STREAM nullable_key_without_final_mark;
DROP STREAM nullable_minmax_index;

DROP STREAM IF EXISTS xxxx_null;
create stream xxxx_null (`ts` Nullable(datetime)) ENGINE = MergeTree ORDER BY to_start_of_hour(ts) SETTINGS allow_nullable_key = 1;
INSERT INTO xxxx_null SELECT '2021-11-11 00:00:00';
SELECT * FROM xxxx_null WHERE ts > '2021-10-11 00:00:00';
DROP STREAM xxxx_null;

-- nullable keys are forbidden when `allow_nullable_key = 0`
create stream invalid_null (id Nullable(string)) ENGINE = MergeTree ORDER BY id; -- { serverError 44 }
create stream invalid_lc_null (id low_cardinality(Nullable(string))) ENGINE = MergeTree ORDER BY id; -- { serverError 44 }
create stream invalid_array_null (id array(Nullable(string))) ENGINE = MergeTree ORDER BY id; -- { serverError 44 }
create stream invalid_tuple_null (id tuple(Nullable(string), uint8)) ENGINE = MergeTree ORDER BY id; -- { serverError 44 }
create stream invalid_map_null (id Map(uint8, Nullable(string))) ENGINE = MergeTree ORDER BY id; -- { serverError 44 }
create stream invalid_simple_agg_state_null (id SimpleAggregateFunction(sum, Nullable(uint64))) ENGINE = MergeTree ORDER BY id; -- { serverError 44 }
-- AggregateFunctions are not comparable and cannot be used in key expressions. No need to test it.
