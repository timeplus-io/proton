SET cast_keep_nullable = 0;

SELECT CAST('Hello' AS LowCardinality(Nullable(string)));
SELECT CAST(Null AS LowCardinality(Nullable(string)));
SELECT CAST(CAST('Hello' AS LowCardinality(Nullable(string))) AS string);
SELECT CAST(CAST(Null AS LowCardinality(Nullable(string))) AS string); -- { serverError 349 }
SELECT CAST(CAST('Hello' AS Nullable(string)) AS string);
SELECT CAST(CAST(Null AS Nullable(string)) AS string); -- { serverError 349 }
