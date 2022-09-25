DROP STREAM IF EXISTS visits;
create stream visits (str string) ENGINE = MergeTree ORDER BY (str);

SELECT 1
FROM visits
ARRAY JOIN array_filter(t -> 1, array_map(x -> (x), [42])) AS i
WHERE ((str, i.1) IN ('x', 0));

DROP STREAM visits;
