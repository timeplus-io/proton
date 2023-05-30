SELECT t1.*, t2.* FROM (SELECT 1 AS k) as t1 JOIN (SELECT -1 AS k) as t2 ON t1.k = t2.k;
