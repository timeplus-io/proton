SELECT group_bitmap_and(z) as y FROM ( SELECT group_bitmap_state(u) AS z FROM ( SELECT 123 AS u ) AS a1 );
SELECT group_bitmap_and(y) FROM (SELECT group_bitmap_and_state(z) as y FROM ( SELECT group_bitmap_state(u) AS z FROM ( SELECT 123 AS u ) AS a1 ) AS a2);

SELECT group_bitmap_and(z) FROM ( SELECT min_state(u) AS z FROM ( SELECT 123 AS u ) AS a1 ) AS a2; -- { serverError 43 }
SELECT group_bitmap_or(z) FROM ( SELECT max_state(u) AS z FROM ( SELECT '123' AS u ) AS a1 ) AS a2; -- { serverError 43 }
SELECT group_bitmap_xor(z) FROM ( SELECT count_state() AS z FROM ( SELECT '123' AS u ) AS a1 ) AS a2; -- { serverError 43 }
