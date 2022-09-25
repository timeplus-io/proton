SELECT arr, element FROM (SELECT [1] AS arr) LEFT ARRAY JOIN arr AS element;
SELECT arr, element FROM (SELECT empty_array_uint8() AS arr) LEFT ARRAY JOIN arr AS element;
SELECT arr, element FROM (SELECT array_join([empty_array_uint8(), [1], [2, 3]]) AS arr) LEFT ARRAY JOIN arr AS element;
