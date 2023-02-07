-- Tags: no-fasttest
SELECT json_extract('{"a" : {"b" : {"c" : 1, "d" : "str"}}}', 'tuple( a low_cardinality(string), b low_cardinality(string), c low_cardinality(string), d low_cardinality(string))');
SELECT json_extract('{"a" : {"b" : {"c" : 1, "d" : "str"}}}', 'tuple( a string, b low_cardinality(string), c low_cardinality(string), d low_cardinality(string))');
