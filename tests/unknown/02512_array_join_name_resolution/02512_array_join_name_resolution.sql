DROP STREAM IF EXISTS x;
CREATE STREAM x ( `arr.key` array(string), `arr.value` array(string), `n` string ) ENGINE = Memory;
INSERT INTO x VALUES (['Hello', 'World'], ['abc', 'def'], 'test');

SELECT
    key,
    any(to_string(n))
FROM
(
    SELECT
        arr.key AS key,
        n
    FROM x
    ARRAY JOIN arr
)
GROUP BY key
ORDER BY key;

DROP STREAM x;
