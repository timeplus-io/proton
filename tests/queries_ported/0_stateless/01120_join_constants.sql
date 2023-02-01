SELECT
    t1.*,
    t2.*,
    'world',
    is_constant('world')
FROM
(
    SELECT
        array_join([1, 2]) AS k,
        'hello'
) AS t1
LEFT JOIN
(
    SELECT
        array_join([1, 3]) AS k,
        'world'
) AS t2 ON t1.k = t2.k ORDER BY t1.k;

SELECT
    t1.*,
    t2.*,
    123,
    is_constant('world')
FROM
(
    SELECT
        array_join([1, 2]) AS k,
        321
) AS t1
LEFT JOIN
(
    SELECT
        array_join([1, 3]) AS k,
        123
) AS t2 ON t1.k = t2.k ORDER BY t1.k;
