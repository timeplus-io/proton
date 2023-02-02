SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON (array_join([1]) = B.b); -- { serverError 403 }
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON (A.a = array_join([1])); -- { serverError 403 }

SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON equals(a); -- { serverError 62 }
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON less(a); -- { serverError 62 }

SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON a = b AND a > b; -- { serverError 403 }
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON a = b AND a < b; -- { serverError 403 }
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON a = b AND a >= b; -- { serverError 403 }
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b) as B ON a = b AND a <= b; -- { serverError 403 }

SET join_algorithm = 'partial_merge';
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b, 1 as c) as B ON a = b OR a = c; -- { serverError 48 }
-- works for a = b OR a = b because of equivalent disjunct optimization

SET join_algorithm = 'auto';
SELECT 1 FROM (select 1 as a) as A JOIN (select 1 as b, 1 as c) as B ON a = b OR a = c; -- { serverError 48 }
-- works for a = b OR a = b because of equivalent disjunct optimization

SET join_algorithm = 'hash';

-- conditions for different stream joined via OR
SELECT * FROM (SELECT 1 AS a, 1 AS b, 1 AS c) AS t1 INNER JOIN (SELECT 1 AS a, 1 AS b, 1 AS c) AS t2 ON t1.a = t2.a AND (t1.b > 0 OR t2.b > 0); -- { serverError 403 }
