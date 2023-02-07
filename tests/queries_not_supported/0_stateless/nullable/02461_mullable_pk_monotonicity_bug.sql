create stream tab (x nullable(uint8)) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);
set allow_suspicious_low_cardinality_types=1;
set max_rows_to_read = 2;

SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::nullable(uint8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(uint8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(nullable(uint8))) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::nullable(uint8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(uint8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(nullable(uint8)), x) <= 2 order by x;

drop stream tab;
set max_rows_to_read = 100;
create stream tab (x low_cardinality(uint8)) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::nullable(uint8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(uint8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(nullable(uint8))) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::nullable(uint8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(uint8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(nullable(uint8)), x) <= 2 order by x;

drop stream tab;
set max_rows_to_read = 100;
create stream tab (x uint128) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::nullable(uint8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(uint8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(nullable(uint8))) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::nullable(uint8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(uint8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(nullable(uint8)), x) <= 2 order by x;

set max_rows_to_read = 100;
SELECT x + 1 FROM tab WHERE (x + 1::low_cardinality(uint8)) <= -9223372036854775808 order by x;

drop stream tab;
create stream tab (x DateTime) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select to_datetime('2022-02-02') + number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= to_datetime('2022-02-02') + 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::nullable(uint8)) <= to_datetime('2022-02-02') + 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(uint8)) <= to_datetime('2022-02-02') + 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::low_cardinality(nullable(uint8))) <= to_datetime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= to_datetime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1::nullable(uint8), x) <= to_datetime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(uint8), x) <= to_datetime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1::low_cardinality(nullable(uint8)), x) <= to_datetime('2022-02-02') + 2 order by x;

SELECT x + 1 FROM tab WHERE (x + CAST('1', 'nullable(uint8)')) <= -2147483647 ORDER BY x ASC NULLS FIRST;
