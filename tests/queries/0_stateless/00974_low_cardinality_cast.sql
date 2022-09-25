SET cast_keep_nullable = 0;

SELECT CAST('Hello' AS low_cardinality(nullable(string)));
SELECT CAST(Null AS low_cardinality(nullable(string)));
SELECT CAST(CAST('Hello' AS low_cardinality(nullable(string))) AS string);
SELECT CAST(CAST(Null AS low_cardinality(nullable(string))) AS string); -- { serverError 349 }
SELECT CAST(CAST('Hello' AS nullable(string)) AS string);
SELECT CAST(CAST(Null AS nullable(string)) AS string); -- { serverError 349 }
