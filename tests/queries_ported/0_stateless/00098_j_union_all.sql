SELECT * FROM (SELECT dummy, -1 as x UNION ALL SELECT dummy, array_join([-1]) as x);
SELECT * FROM (SELECT -1 as x, dummy UNION ALL SELECT array_join([-1]) as x, dummy);
