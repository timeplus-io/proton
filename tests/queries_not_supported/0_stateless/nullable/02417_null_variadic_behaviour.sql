-- { echo }
SELECT avg_weighted(number, number) as t, to_type_name(t) FROM numbers(1);
SELECT avg_weighted(number, number + 1) as t, to_type_name(t) FROM numbers(0);

SELECT avg_weighted(to_nullable(number), number) as t, to_type_name(t) FROM numbers(1);
SELECT avg_weighted(if(number < 10000, NULL, number), number) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(if(number < 50, NULL, number), number) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weighted(number, if(number < 10000, NULL, number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(number, if(number < 50, NULL, number)) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weighted(to_nullable(number), if(number < 10000, NULL, number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(to_nullable(number), if(number < 50, NULL, number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(if(number < 10000, NULL, number), to_nullable(number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(if(number < 50, NULL, number), to_nullable(number)) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weighted(to_nullable(number), if(number < 500, NULL, number)) as t, to_type_name(t) FROM numbers(1000);

SELECT avg_weighted(if(number < 10000, NULL, number), if(number < 10000, NULL, number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(if(number < 50, NULL, number), if(number < 10000, NULL, number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(if(number < 10000, NULL, number), if(number < 50, NULL, number)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weighted(if(number < 50, NULL, number), if(number < 50, NULL, number)) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weighted(if(number < 10000, NULL, number), if(number < 500, NULL, number)) as t, to_type_name(t) FROM numbers(1000);

SELECT avg_weightedIf(number, number, number % 10) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(number, number, to_nullable(number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(number, number, if(number < 10000, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(number, number, if(number < 50, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(number, number, if(number < 0, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weightedIf(number, number, to_nullable(number % 10)) as t, to_type_name(t) FROM numbers(1000);

SELECT avg_weightedIf(if(number < 10000, NULL, number), if(number < 10000, NULL, number), if(number < 10000, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 50, NULL, number), if(number < 10000, NULL, number), if(number < 10000, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 10000, NULL, number), if(number < 50, NULL, number), if(number < 10000, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 50, NULL, number), if(number < 50, NULL, number), if(number < 10000, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weightedIf(if(number < 10000, NULL, number), if(number < 10000, NULL, number), if(number < 50, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 50, NULL, number), if(number < 10000, NULL, number), if(number < 50, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 10000, NULL, number), if(number < 50, NULL, number), if(number < 50, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 50, NULL, number), if(number < 50, NULL, number), if(number < 50, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);

SELECT avg_weightedIf(if(number < 10000, NULL, number), if(number < 10000, NULL, number), if(number < 0, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 50, NULL, number), if(number < 10000, NULL, number), if(number < 0, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 10000, NULL, number), if(number < 50, NULL, number), if(number < 0, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
SELECT avg_weightedIf(if(number < 50, NULL, number), if(number < 50, NULL, number), if(number < 0, NULL, number % 10)) as t, to_type_name(t) FROM numbers(100);
