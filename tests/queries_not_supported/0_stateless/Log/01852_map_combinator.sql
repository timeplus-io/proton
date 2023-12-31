SET allow_experimental_map_type = 1;

DROP STREAM IF EXISTS map_comb;
CREATE STREAM map_comb(a int, statusMap map(uint16, uint32)) ENGINE = Log;

INSERT INTO map_comb VALUES (1, map(1, 10, 2, 10, 3, 10)),(1, map(3, 10, 4, 10, 5, 10)),(2, map(4, 10, 5, 10, 6, 10)),(2, map(6, 10, 7, 10, 8, 10)),(3, map(1, 10, 2, 10, 3, 10)),(4, map(3, 10, 4, 10, 5, 10)),(5, map(4, 10, 5, 10, 6, 10)),(5, map(6, 10, 7, 10, 8, 10));

SELECT * FROM map_comb ORDER BY a, statusMap;
SELECT to_type_name(res), sumMap(statusMap) as res FROM map_comb;
SELECT to_type_name(res), sumWithOverflowMap(statusMap) as res FROM map_comb;
SELECT to_type_name(res), sumMapMerge(s) as res FROM (SELECT sumMapState(statusMap) AS s FROM map_comb);
SELECT minMap(statusMap) FROM map_comb;
SELECT maxMap(statusMap) FROM map_comb;
SELECT to_type_name(res), avgMap(statusMap) as res FROM map_comb;
SELECT countMap(statusMap) FROM map_comb;
SELECT a, sumMap(statusMap) FROM map_comb GROUP BY a ORDER BY a;

DROP STREAM map_comb;

-- check different types
select minMap(val) from values ('val map(uuid, int32)',
	(map('01234567-89ab-cdef-0123-456789abcdef', 1)),
	(map('01234567-89ab-cdef-0123-456789abcdef', 2)));
select minMap(val) from values ('val map(string, string)',  (map('1', '1')), (map('1', '2')));
select minMap(val) from values ('val map(fixed_string(1), fixed_string(1))',  (map('1', '1')), (map('1', '2')));
select minMap(val) from values ('val map(uint64, uint64)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val map(Date, int16)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val map(DateTime(\'Asia/Istanbul\'), int32)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val map(enum16(\'a\'=1), int16)',  (map('a', 1)), (map('a', 2)));
select maxMap(val) from values ('val map(string, string)',  (map('1', '1')), (map('1', '2')));
select minMap(val) from values ('val map(int128, int128)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val map(int256, int256)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val map(uint128, uint128)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val map(uint256, uint256)',  (map(1, 1)), (map(1, 2)));

select sumMap(map(1,2), 1, 2); -- { serverError 42 }
select sumMap(map(1,2), map(1,3)); -- { serverError 42 }

-- array and tuple arguments
select avgMap([1,1,1], [2,2,2]); -- { serverError 43 }
select minMap((1,1)); -- { serverError 43 }
select minMap(([1,1,1],1)); -- { serverError 43 }
select minMap([1,1,1],1); -- { serverError 43 }
select minMap([1,1,1]); -- { serverError 43 }
select minMap(([1,1,1])); -- { serverError 43 }

DROP STREAM IF EXISTS sum_map_decimal;

CREATE STREAM sum_map_decimal(statusMap map(uint16,decimal32(5))) ENGINE = Log;

INSERT INTO sum_map_decimal VALUES (map(1,'1.0',2,'2.0',3,'3.0')), (map(3,'3.0',4,'4.0',5,'5.0')), (map(4,'4.0',5,'5.0',6,'6.0')), (map(6,'6.0',7,'7.0',8,'8.0'));

SELECT sumMap(statusMap) FROM sum_map_decimal;
SELECT sumWithOverflowMap(statusMap) FROM sum_map_decimal;

DROP STREAM sum_map_decimal;
