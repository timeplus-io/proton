SELECT x FROM (SELECT DISTINCT 1 AS x, array_join([1, 2]) AS y)
