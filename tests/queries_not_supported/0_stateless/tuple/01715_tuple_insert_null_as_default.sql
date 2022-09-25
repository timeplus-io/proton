SELECT 'tuple';

DROP STREAM IF EXISTS test_tuple;
create stream test_tuple (value tuple(uint8, uint8)) ;

SET input_format_null_as_default = 1;
INSERT INTO test_tuple VALUES ((NULL, 1));
SELECT * FROM test_tuple;

SET input_format_null_as_default = 0;
INSERT INTO test_tuple VALUES ((NULL, 2)); -- { clientError 53 }
SELECT * FROM test_tuple;

DROP STREAM test_tuple;

SELECT 'tuple nested in array';

DROP STREAM IF EXISTS test_tuple_nested_in_array;
create stream test_tuple_nested_in_array (value array(tuple(uint8, uint8))) ;

SET input_format_null_as_default = 1;
INSERT INTO test_tuple_nested_in_array VALUES ([(NULL, 2), (3, NULL), (NULL, 4)]);
SELECT * FROM test_tuple_nested_in_array;

SET input_format_null_as_default = 0;
INSERT INTO test_tuple_nested_in_array VALUES ([(NULL, 1)]); -- { clientError 53 }
SELECT * FROM test_tuple_nested_in_array;

DROP STREAM test_tuple_nested_in_array;

SELECT 'tuple nested in array nested in tuple';

DROP STREAM IF EXISTS test_tuple_nested_in_array_nested_in_tuple;
create stream test_tuple_nested_in_array_nested_in_tuple (value tuple(uint8, array(tuple(uint8, uint8)))) ;

SET input_format_null_as_default = 1;
INSERT INTO test_tuple_nested_in_array_nested_in_tuple VALUES ( (NULL, [(NULL, 2), (3, NULL), (NULL, 4)]) );
SELECT * FROM test_tuple_nested_in_array_nested_in_tuple;

SET input_format_null_as_default = 0;
INSERT INTO test_tuple_nested_in_array_nested_in_tuple VALUES ( (NULL, [(NULL, 1)]) ); -- { clientError 53 }
SELECT * FROM test_tuple_nested_in_array_nested_in_tuple;

DROP STREAM test_tuple_nested_in_array_nested_in_tuple;

SELECT 'tuple nested in Map';

SET allow_experimental_map_type = 1;

DROP STREAM IF EXISTS test_tuple_nested_in_map;
create stream test_tuple_nested_in_map (value Map(string, tuple(uint8, uint8))) ;

SET input_format_null_as_default = 1;
INSERT INTO test_tuple_nested_in_map VALUES (map('test', (NULL, 1)));

SELECT * FROM test_tuple_nested_in_map;

SET input_format_null_as_default = 0;
INSERT INTO test_tuple_nested_in_map VALUES (map('test', (NULL, 1))); -- { clientError 53 }
SELECT * FROM test_tuple_nested_in_map;

DROP STREAM test_tuple_nested_in_map;

SELECT 'tuple nested in Map nested in tuple';

DROP STREAM IF EXISTS test_tuple_nested_in_map_nested_in_tuple;
create stream test_tuple_nested_in_map_nested_in_tuple (value tuple(uint8, Map(string, tuple(uint8, uint8)))) ;

SET input_format_null_as_default = 1;
INSERT INTO test_tuple_nested_in_map_nested_in_tuple VALUES ( (NULL, map('test', (NULL, 1))) );
SELECT * FROM test_tuple_nested_in_map_nested_in_tuple;

SET input_format_null_as_default = 0;
INSERT INTO test_tuple_nested_in_map_nested_in_tuple VALUES ( (NULL, map('test', (NULL, 1))) ); -- { clientError 53 }
SELECT * FROM test_tuple_nested_in_map_nested_in_tuple;

DROP STREAM test_tuple_nested_in_map_nested_in_tuple;
