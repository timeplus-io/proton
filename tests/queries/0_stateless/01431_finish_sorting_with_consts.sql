DROP STREAM IF EXISTS pk_func;

create stream pk_func (`d` DateTime, `ui` uint32 ) ENGINE = MergeTree ORDER BY to_date(d);
INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(1000);
SELECT 1, * FROM pk_func ORDER BY to_date(d) ASC, ui ASC LIMIT 3;

DROP STREAM IF EXISTS pk_func;
