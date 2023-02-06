DROP STREAM IF EXISTS tab;

CREATE STREAM tab (haystack string, pattern string) engine = MergeTree() ORDER BY haystack;

INSERT INTO tab VALUES ('haystack', 'pattern\\');

-- const pattern
SELECT haystack LIKE 'pattern\\' from tab; -- { serverError 25 }

-- non-const pattern
SELECT haystack LIKE pattern from tab; -- { serverError 25 }

DROP STREAM IF EXISTS tab;
