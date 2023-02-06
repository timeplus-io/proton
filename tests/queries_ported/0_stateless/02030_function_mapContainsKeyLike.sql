DROP STREAM IF EXISTS map_containsKeyLike_test;

CREATE STREAM map_containsKeyLike_test (id uint32, map map(string, string)) Engine=MergeTree() ORDER BY id settings index_granularity=2;

INSERT INTO map_containsKeyLike_test VALUES (1, {'1-K1':'1-V1','1-K2':'1-V2'}),(2,{'2-K1':'2-V1','2-K2':'2-V2'});
INSERT INTO map_containsKeyLike_test VALUES (3, {'3-K1':'3-V1','3-K2':'3-V2'}),(4, {'4-K1':'4-V1','4-K2':'4-V2'});
INSERT INTO map_containsKeyLike_test VALUES (5, {'5-K1':'5-V1','5-K2':'5-V2'}),(6, {'6-K1':'6-V1','6-K2':'6-V2'});

SELECT id, map FROM map_containsKeyLike_test WHERE map_contains_key_like(map, '1-%') = 1;
SELECT id, map FROM map_containsKeyLike_test WHERE map_contains_key_like(map, '3-%') = 0 order by id;

DROP STREAM map_containsKeyLike_test;
