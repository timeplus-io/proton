DROP STREAM IF EXISTS map_extractKeyLike_test;

CREATE STREAM map_extractKeyLike_test (id uint32, map map(string, string)) Engine=MergeTree() ORDER BY id settings index_granularity=2;

INSERT INTO map_extractKeyLike_test VALUES (1, {'P1-K1':'1-V1','P2-K2':'1-V2'}),(2,{'P1-K1':'2-V1','P2-K2':'2-V2'});
INSERT INTO map_extractKeyLike_test VALUES (3, {'P1-K1':'3-V1','P2-K2':'3-V2'}),(4,{'P1-K1':'4-V1','P2-K2':'4-V2'});
INSERT INTO map_extractKeyLike_test VALUES (5, {'5-K1':'5-V1','5-K2':'5-V2'}),(6, {'P3-K1':'6-V1','P4-K2':'6-V2'});

SELECT 'The data of stream:';
SELECT * FROM map_extractKeyLike_test ORDER BY id; 

SELECT '';

SELECT 'The results of query: SELECT id, map_extract_key_like(map, \'P1%\') FROM map_extractKeyLike_test ORDER BY id;';
SELECT id, map_extract_key_like(map, 'P1%') FROM map_extractKeyLike_test ORDER BY id;

SELECT '';

SELECT 'The results of query: SELECT id, map_extract_key_like(map, \'5-K1\') FROM map_extractKeyLike_test ORDER BY id;';
SELECT id, map_extract_key_like(map, '5-K1') FROM map_extractKeyLike_test ORDER BY id;

DROP STREAM map_extractKeyLike_test;
