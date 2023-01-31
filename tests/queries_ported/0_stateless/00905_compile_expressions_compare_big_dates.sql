SET query_mode='table';
SET asterisk_include_reserved_columns=false;
SET compile_expressions = 1;
SET min_count_to_compile_expression = 1;

DROP TABLE IF EXISTS foo_c;

CREATE TABLE foo_c(d DateTime) ENGINE = Memory;

INSERT INTO foo_c(d) VALUES ('2019-02-06 01:01:01'),('2019-02-07 01:01:01'),('2019-02-08 01:01:01'),('2021-02-06 01:01:01'),('2093-05-29 01:01:01'),('2100-06-06 01:01:01'),('2100-10-14 01:01:01'),('2100-11-01 01:01:01'),('2100-11-15 01:01:01'),('2100-11-30 01:01:01'),('2100-12-11 01:01:01'),('2100-12-21 01:01:01');

<<<<<<< HEAD:tests/queries/0_stateless/00905_compile_expressions_compare_big_dates.sql
SELECT toDate(d) AS dd FROM foo_c WHERE (dd >= '2019-02-06') AND (toDate(d) <= toDate('2019-08-09')) GROUP BY dd ORDER BY dd;
=======
select sleep(3);
SELECT to_date(d) AS dd FROM foo_c WHERE (dd >= '2019-02-06') AND (to_date(d) <= to_date('2019-08-09')) GROUP BY dd ORDER BY dd;
>>>>>>> 9e73b005c8... CH porting case ,v3:tests/queries_ported/0_stateless/00905_compile_expressions_compare_big_dates.sql

SELECT toDate(d) FROM foo_c WHERE (d > toDate('2019-02-10')) AND (d <= toDate('2022-01-01')) ORDER BY d;

DROP TABLE IF EXISTS foo_c;
