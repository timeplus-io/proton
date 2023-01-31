-- Tags: no-debug

SET allow_hyperscan = 1;
SELECT multi_match_any(array_join(['hello', 'world', 'hellllllllo', 'wororld', 'abc']), ['hel+o', 'w(or)*ld']);
SET allow_hyperscan = 0;
SELECT multi_match_any(array_join(['hello', 'world', 'hellllllllo', 'wororld', 'abc']), ['hel+o', 'w(or)*ld']); -- { serverError 446 }
SELECT multi_match_all_indices(array_join(['hello', 'world', 'hellllllllo', 'wororld', 'abc']), ['hel+o', 'w(or)*ld']); -- { serverError 446 }

SELECT multi_search_any(array_join(['hello', 'world', 'hello, world', 'abc']), ['hello', 'world']);
