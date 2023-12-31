select * from (select number, count() from numbers(2) group by number with totals) where number > 0 settings enable_optimize_predicate_expression=0;

select '-';

CREATE STREAM foo (server_date Date, dimension_1 string, metric_1 uint32) ENGINE = MergeTree() PARTITION BY to_YYYYMM(server_date) ORDER BY (server_date);
CREATE STREAM bar (server_date Date, dimension_1 string, metric_2 uint32) ENGINE = MergeTree() PARTITION BY to_YYYYMM(server_date) ORDER BY (server_date);

INSERT INTO foo VALUES ('2020-01-01', 'test1', 10), ('2020-01-01', 'test2', 20);
INSERT INTO bar VALUES ('2020-01-01', 'test2', 30), ('2020-01-01', 'test3', 40);

SELECT
    dimension_1,
    sum_metric_1,
    sum_metric_2
FROM
(
    SELECT
        dimension_1,
        sum(metric_1) AS sum_metric_1
    FROM foo
    GROUP BY dimension_1
        WITH TOTALS
) AS subquery_1
ALL FULL OUTER JOIN
(
    SELECT
        dimension_1,
        sum(metric_2) AS sum_metric_2
    FROM bar
    GROUP BY dimension_1
        WITH TOTALS
) AS subquery_2 USING (dimension_1)
WHERE sum_metric_2 < 20
ORDER BY dimension_1 ASC;

DROP STREAM foo;
DROP STREAM bar;
