-- NOTE: database = currentDatabase() is not mandatory

SELECT database FROM system.tables WHERE database LIKE '%' format Null;
SELECT database AS db FROM system.tables WHERE db LIKE '%' format Null;
SELECT CAST(database, 'string') AS db FROM system.tables WHERE db LIKE '%' format Null;
SELECT CAST('a string', 'Nullable(string)') AS str WHERE str LIKE '%' format Null;
SELECT CAST(database, 'Nullable(string)') AS ndb FROM system.tables WHERE ndb LIKE '%' format Null;
SELECT 'all tests passed';
