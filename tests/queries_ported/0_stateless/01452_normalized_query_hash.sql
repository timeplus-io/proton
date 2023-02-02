SELECT normalized_query_hash('SELECT 1') = normalized_query_hash('SELECT 2');
SELECT normalized_query_hash('SELECT  1') != normalized_query_hash('SELECT  1, 1, 1');
SELECT normalized_query_hash('SELECT 1, 1, 1, /* Hello */ \'abc\'') = normalized_query_hash('SELECT 2, 3');
SELECT normalized_query_hash('[1, 2, 3]') = normalized_query_hash('[1, ''x'']');
SELECT normalized_query_hash('[1, 2, 3, x]') != normalized_query_hash('[1, x]');
SELECT normalized_query_hash('SELECT 1 AS `xyz`') != normalized_query_hash('SELECT 1 AS `abc`');
SELECT normalized_query_hash('SELECT 1 AS xyz111') = normalized_query_hash('SELECT 2 AS xyz234');
SELECT normalized_query_hash('SELECT $doc$VALUE$doc$ AS `xyz`') != normalized_query_hash('SELECT $doc$VALUE$doc$ AS `abc`');
SELECT normalized_query_hash('SELECT $doc$VALUE$doc$ AS xyz111') = normalized_query_hash('SELECT $doc$VALUE$doc$ AS xyz234');


