DROP STREAM IF EXISTS grouping_sets;

CREATE STREAM grouping_sets(fact_1_id int32, fact_2_id int32, fact_3_id int32, fact_4_id int32, sales_value int32) ENGINE = Memory;

SELECT fact_1_id, fact_3_id, sum(sales_value), count() from grouping_sets GROUP BY GROUPING SETS(fact_1_id, fact_3_id) ORDER BY fact_1_id, fact_3_id;

INSERT INTO grouping_sets
SELECT
    number % 2 + 1 AS fact_1_id,
       number % 5 + 1 AS fact_2_id,
       number % 10 + 1 AS fact_3_id,
       number % 10 + 1 AS fact_4_id,
       number % 100 AS sales_value
FROM system.numbers limit 1000;

EXPLAIN PIPELINE
SELECT fact_1_id, fact_2_id, fact_3_id, sum(sales_value) AS sales_value from grouping_sets
GROUP BY GROUPING SETS ((fact_1_id, fact_2_id), (fact_1_id, fact_3_id))
ORDER BY fact_1_id, fact_2_id, fact_3_id;

SELECT fact_1_id, fact_2_id, fact_3_id, sum(sales_value) AS sales_value from grouping_sets
GROUP BY GROUPING SETS ((fact_1_id, fact_2_id), (fact_1_id, fact_3_id))
ORDER BY fact_1_id, fact_2_id, fact_3_id;

SELECT fact_1_id, fact_2_id, fact_3_id, fact_4_id, sum(sales_value) AS sales_value from grouping_sets
GROUP BY GROUPING SETS ((fact_1_id, fact_2_id), (fact_3_id, fact_4_id))
ORDER BY fact_1_id, fact_2_id, fact_3_id, fact_4_id;

SELECT fact_1_id, fact_2_id, fact_3_id, sum(sales_value) AS sales_value from grouping_sets
GROUP BY GROUPING SETS ((fact_1_id, fact_2_id), (fact_3_id), ())
ORDER BY fact_1_id, fact_2_id, fact_3_id;

SELECT
    fact_1_id,
    fact_3_id,
    sum(sales_value) AS sales_value
FROM grouping_sets
GROUP BY grouping sets ((fact_1_id), (fact_1_id, fact_3_id)) WITH TOTALS
ORDER BY fact_1_id, fact_3_id; -- { serverError NOT_IMPLEMENTED }

EXPLAIN SYNTAX SELECT
    fact_1_id,
    fact_3_id,
    sum(sales_value) AS sales_value
FROM grouping_sets
GROUP BY grouping sets (fact_1_id, (fact_1_id, fact_3_id)) WITH TOTALS
ORDER BY fact_1_id, fact_3_id;

SELECT
    fact_1_id,
    fact_3_id,
    sum(sales_value) AS sales_value
FROM grouping_sets
GROUP BY grouping sets (fact_1_id, (fact_1_id, fact_3_id)) WITH TOTALS
ORDER BY fact_1_id, fact_3_id; -- { serverError NOT_IMPLEMENTED }

DROP STREAM grouping_sets;

EXPLAIN PIPELINE
SELECT sum(number) as sum_value, count() AS count_value from numbers_mt(1000000)
GROUP BY GROUPING SETS ((number % 10), (number % 100))
ORDER BY sum_value, count_value SETTINGS max_threads=3;

SELECT sum(number) as sum_value, count() AS count_value from numbers_mt(1000000)
GROUP BY GROUPING SETS ((number % 10), (number % 100))
ORDER BY sum_value, count_value SETTINGS max_threads=3;
