SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS partition_and_primary_keys_using_same_expression;

create stream partition_and_primary_keys_using_same_expression(dt datetime)
    ENGINE MergeTree PARTITION BY to_date(dt) ORDER BY to_day_of_week(to_date(dt));

INSERT INTO partition_and_primary_keys_using_same_expression(dt)
    VALUES ('2018-02-19 12:00:00');
INSERT INTO partition_and_primary_keys_using_same_expression(dt)
    VALUES ('2018-02-20 12:00:00'), ('2018-02-21 12:00:00');
SELECT sleep(3);
SELECT * FROM partition_and_primary_keys_using_same_expression ORDER BY dt;

SELECT '---';

ALTER STREAM partition_and_primary_keys_using_same_expression DROP PARTITION '2018-02-20';
SELECT * FROM partition_and_primary_keys_using_same_expression ORDER BY dt;

DROP STREAM partition_and_primary_keys_using_same_expression;
