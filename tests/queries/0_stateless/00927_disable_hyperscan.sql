-- Tags: no-debug

SET allow_hyperscan = 1;
SELECT multiMatchAny(array_join(['hello', 'world', 'hellllllllo', 'wororld', 'abc']), ['hel+o', 'w(or)*ld']);
SET allow_hyperscan = 0;
SELECT multiMatchAny(array_join(['hello', 'world', 'hellllllllo', 'wororld', 'abc']), ['hel+o', 'w(or)*ld']); -- { serverError 446 }
SELECT multiMatchAllIndices(array_join(['hello', 'world', 'hellllllllo', 'wororld', 'abc']), ['hel+o', 'w(or)*ld']); -- { serverError 446 }

SELECT multiSearchAny(array_join(['hello', 'world', 'hello, world', 'abc']), ['hello', 'world']);
