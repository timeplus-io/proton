SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS aggregates;
create stream aggregates (d date, s aggregate_function(uniq, uint64)) ENGINE = MergeTree(d, d, 8192);

INSERT INTO aggregates
    SELECT to_date('2016-10-31') AS d, uniq_state(to_uint64(array_join(range(100)))) AS s
    UNION ALL
    SELECT to_date('2016-11-01') AS d, uniq_state(to_uint64(array_join(range(100)))) AS s;

INSERT INTO aggregates SELECT to_date('2016-10-31') + number AS d, uniq_state(to_uint64(array_join(range(100)))) AS s FROM (SELECT * FROM system.numbers LIMIT 2) GROUP BY d;
SELECT sleep(3);

SELECT d, uniqMerge(s) FROM aggregates GROUP BY d ORDER BY d;

INSERT INTO aggregates
    SELECT to_date('2016-12-01') AS d, uniq_state(to_uint64(array_join(range(100)))) AS s
    UNION ALL
    SELECT to_date('2016-12-02') AS d, uniq_state(to_uint64(array_join(range(100)))) AS s
    UNION ALL
    SELECT to_date('2016-12-03') AS d, uniq_state(to_uint64(array_join(range(100)))) AS s;
SELECT sleep(3);
SELECT d, uniqMerge(s) FROM aggregates GROUP BY d ORDER BY d;

DROP STREAM aggregates;
