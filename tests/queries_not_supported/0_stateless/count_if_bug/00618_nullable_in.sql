SELECT sum(toNullable('a') IN 'a');
SELECT count_if(number, toNullable('a') IN ('a', 'b')) FROM numbers(100);

SELECT
    uniqExact(x) AS u, 
    uniqExactIf(x, name = 'a') AS ue, 
    uniqExactIf(x, name IN ('a', 'b')) AS ui
FROM
(
    SELECT
        toNullable('a') AS name, 
        array_join(range(10)) AS x
) 
WHERE name = 'a';
