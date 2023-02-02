SET optimize_aggregators_of_group_by_keys = 1;
SELECT source.key, max(target.key) FROM (SELECT 1 as key, 'x' as name) as source
INNER JOIN (SELECT 2 as key, 'x' as name) as target
ON source.name = target.name
GROUP BY source.key;
