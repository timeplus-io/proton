-- Unneeded column is removed from subquery.
SELECT count() FROM (SELECT number, group_array(repeat(to_string(number), 1000000)) FROM numbers(1000000) GROUP BY number);
-- Unneeded column cannot be removed from subquery and the query is out of memory
SELECT count() FROM (SELECT number, group_array(repeat(to_string(number), 1000000)) AS agg FROM numbers(1000000) GROUP BY number HAVING not_empty(agg)); -- { serverError 241 }
