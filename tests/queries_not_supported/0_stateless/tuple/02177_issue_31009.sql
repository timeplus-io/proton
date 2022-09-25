-- Tags: long

create stream left ( key uint32, value string ) ENGINE = MergeTree ORDER BY key;
create stream right (  key uint32, value string ) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left SELECT number, to_string(number) FROM numbers(25367182);
INSERT INTO right SELECT number, to_string(number) FROM numbers(23124707);

SET join_algorithm = 'partial_merge';

SELECT key, count(1) AS cnt
FROM (
    SELECT data.key
    FROM ( SELECT key FROM left AS s ) AS data
    LEFT JOIN ( SELECT key FROM right GROUP BY key ) AS promo ON promo.key = data.key
) GROUP BY key HAVING count(1) > 1;

DROP STREAM IF EXISTS left;
DROP STREAM IF EXISTS right;
