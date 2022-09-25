SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS cast_enums;
create stream cast_enums
(
    type enum8('session' = 1, 'pageview' = 2, 'click' = 3),
    date date,
    id uint64
) ENGINE = MergeTree(date, (type, date, id), 8192);

INSERT INTO cast_enums SELECT 'session' AS type, to_date('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;
INSERT INTO cast_enums SELECT 2 AS type, to_date('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;
SELECT sleep(3);

SELECT type, date, id FROM cast_enums ORDER BY type, id;

INSERT INTO cast_enums VALUES ('wrong_value', '2017-01-02', 7); -- { clientError 36 }

DROP STREAM IF EXISTS cast_enums;
