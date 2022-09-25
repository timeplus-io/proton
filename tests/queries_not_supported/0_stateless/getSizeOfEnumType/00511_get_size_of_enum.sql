SELECT getSizeOfEnumType(CAST(1 AS enum8('a' = 1, 'b' = 2)));
SELECT getSizeOfEnumType(CAST('b' AS Enum16('a' = 1, 'b' = 2, 'x' = 10)));
