SELECT count() FROM numbers(10) WHERE number = -1 WITH TOTALS FORMAT Vertical;
SELECT count() FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
SELECT number, count() FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
SELECT group_array(number) FROM numbers(10) WHERE number = -1 WITH TOTALS FORMAT Vertical;
SELECT group_array(number) FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
SELECT number, group_array(number) FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
