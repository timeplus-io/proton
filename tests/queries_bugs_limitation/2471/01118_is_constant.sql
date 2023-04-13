select is_constant(1);
select is_constant([1]);
select is_constant(array_join([1]));
SELECT is_constant((SELECT 1));
SELECT is_constant(x) FROM (SELECT 1 as x);
SELECT '---';
SELECT is_constant(x) FROM (SELECT 1 as x UNION ALL SELECT 2);
SELECT '---';
select is_constant(); -- { serverError 42 }
select is_constant(1, 2); -- { serverError 42 }
