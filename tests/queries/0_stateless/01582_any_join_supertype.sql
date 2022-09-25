DROP STREAM IF EXISTS foo;
DROP STREAM IF EXISTS bar;

create stream foo (server_date date, server_time Datetime('Europe/Moscow'), dimension_1 string) ENGINE = MergeTree() PARTITION BY to_YYYYMM(server_date) ORDER BY (server_date);
create stream bar (server_date date, dimension_1 string) ENGINE = MergeTree() PARTITION BY to_YYYYMM(server_date) ORDER BY (server_date);

INSERT INTO foo VALUES ('2020-01-01', '2020-01-01 12:00:00', 'test1'), ('2020-01-01', '2020-01-01 13:00:00', 'test2');
INSERT INTO bar VALUES ('2020-01-01', 'test2'), ('2020-01-01', 'test3');

SET optimize_move_to_prewhere = 1;
SET any_join_distinct_right_table_keys = 0;

SELECT count()
FROM foo ANY INNER JOIN bar USING (dimension_1)
WHERE (foo.server_date <= '2020-11-07') AND (to_date(foo.server_time, 'Asia/Yekaterinburg') <= '2020-11-07');

SELECT to_datetime(foo.server_time, 'UTC')
FROM foo
ANY INNER JOIN bar USING (dimension_1)
WHERE to_date(foo.server_time, 'UTC') <= to_date('2020-04-30');

SELECT to_datetime(foo.server_time, 'UTC') FROM foo
SEMI JOIN bar USING (dimension_1) WHERE to_date(foo.server_time, 'UTC') <= to_date('2020-04-30');

SET any_join_distinct_right_table_keys = 1;

SELECT count()
FROM foo ANY INNER JOIN bar USING (dimension_1)
WHERE (foo.server_date <= '2020-11-07') AND (to_date(foo.server_time, 'Asia/Yekaterinburg') <= '2020-11-07');

SELECT to_datetime(foo.server_time, 'UTC')
FROM foo
ANY INNER JOIN bar USING (dimension_1)
WHERE to_date(foo.server_time, 'UTC') <= to_date('2020-04-30');

DROP STREAM foo;
DROP STREAM bar;
