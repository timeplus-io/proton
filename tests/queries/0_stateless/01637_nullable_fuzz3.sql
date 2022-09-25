DROP STREAM IF EXISTS t;
create stream t (`item_id` uint64, `price_sold` float32, `date` date) ENGINE = MergeTree ORDER BY item_id;
SELECT item_id FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) AS l FULL OUTER JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) AS r USING (item_id);
SELECT item_id FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) AS l FULL OUTER JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) AS r USING (item_id) SETTINGS join_use_nulls = '1';
SELECT * FROM (SELECT item_id, sum(price_sold) as price_sold FROM t GROUP BY item_id WITH TOTALS) AS l FULL OUTER JOIN (SELECT item_id, sum(price_sold) as price_sold FROM t GROUP BY item_id WITH TOTALS) AS r USING (item_id) SETTINGS join_use_nulls = '1';
DROP STREAM t;
