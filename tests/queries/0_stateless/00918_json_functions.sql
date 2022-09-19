-- Tags: no-fasttest

SELECT '--allow_simdjson=1--';
SET allow_simdjson=1;

SELECT '--JSONLength--';
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONLength('{}');

SELECT '--JSONHas--';
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'c');

SELECT '--isValidJSON--';
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT isValidJSON('not a json');
SELECT isValidJSON('"HX-=');

SELECT '--JSONKey--';
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2);

SELECT '--JSONType--';
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
SELECT JSONExtractBool('{"passed": true}', 'passed');
SELECT JSONExtractBool('"HX-=');

SELECT '--JSONExtractString--';
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
select JSONExtractString('{"abc":"\\n\\u0000"}', 'abc');
select JSONExtractString('{"abc":"\\u263a"}', 'abc');
select JSONExtractString('{"abc":"\\u263"}', 'abc');
select JSONExtractString('{"abc":"hello}', 'abc');

SELECT '--JSONExtract (generic)--';
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(string, array(float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(a string, b array(float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(b array(float64), a string)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(a FixedString(6), c uint8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'a', 'string');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(Float32)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'tuple(int8, Float32, uint16)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(int8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(Nullable(int8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(uint8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(Nullable(uint8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1, 'int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2, 'int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(int64)');
SELECT JSONExtract('{"passed": true}', 'passed', 'uint8');
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(a int, b int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(c int, a int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(b int, d int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(int, int)');
SELECT JSONExtract('{"a":3}', 'tuple(int, int)');
SELECT JSONExtract('[3,5,7]', 'tuple(int, int)');
SELECT JSONExtract('[3]', 'tuple(int, int)');
SELECT JSONExtract('{"a":123456, "b":3.55}', 'tuple(a LowCardinality(int32), b Decimal(5, 2))');
SELECT JSONExtract('{"a":1, "b":"417ddc5d-e556-4d27-95dd-a34d84e46a50"}', 'tuple(a int8, b UUID)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'a', 'LowCardinality(string)');
SELECT JSONExtract('{"a":3333.6333333333333333333333, "b":"test"}', 'tuple(a Decimal(10,1), b LowCardinality(string))');
SELECT JSONExtract('{"a":3333.6333333333333333333333, "b":"test"}', 'tuple(a Decimal(20,10), b LowCardinality(string))');
SELECT JSONExtract('{"a":123456.123456}', 'a', 'Decimal(20, 4)') as a, to_type_name(a);
SELECT to_decimal64(123456789012345.12, 4), JSONExtract('{"a":123456789012345.12}', 'a', 'Decimal(30, 4)');
SELECT toDecimal128(1234567890.12345678901234567890, 20), JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}', 'tuple(a Decimal(35,20), b LowCardinality(string))');
SELECT toDecimal256(1234567890.123456789012345678901234567890, 30), JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}', 'tuple(a Decimal(45,30), b LowCardinality(string))');

SELECT '--JSONExtractKeysAndValues--';
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'string');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'array(float64)');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}', 'string');
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'int8');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}', 'LowCardinality(string)');

SELECT '--JSONExtractRaw--';
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 2);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 3);
SELECT JSONExtractRaw('{"passed": true}');
SELECT JSONExtractRaw('{}');
SELECT JSONExtractRaw('{"abc":"\\n\\u0000"}', 'abc');
SELECT JSONExtractRaw('{"abc":"\\u263a"}', 'abc');

SELECT '--JSONExtractArrayRaw--';
SELECT JSONExtractArrayRaw('');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": "not_array"}');
SELECT JSONExtractArrayRaw('[]');
SELECT JSONExtractArrayRaw('[[],[]]');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractArrayRaw('[1,2,3,4,5,"hello"]');
SELECT JSONExtractArrayRaw(array_join(JSONExtractArrayRaw('[[1,2,3],[4,5,6]]')));

SELECT '--JSONExtractKeysAndValuesRaw--';
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT array_join([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json, 's') FROM (SELECT array_join(['{"s":"u"}', '{"s":"v"}']) AS json);

SELECT '--show error: type should be const string';
SELECT JSONExtractKeysAndValues([], JSONLength('^?V{LSwp')); -- { serverError 44 }
WITH '{"i": 1, "f": 1.2}' AS json SELECT JSONExtract(json, 'i', JSONType(json, 'i')); -- { serverError 44 }


SELECT '--allow_simdjson=0--';
SET allow_simdjson=0;

SELECT '--JSONLength--';
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONLength('{}');

SELECT '--JSONHas--';
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'c');

SELECT '--isValidJSON--';
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT isValidJSON('not a json');
SELECT isValidJSON('"HX-=');

SELECT '--JSONKey--';
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2);

SELECT '--JSONType--';
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
SELECT JSONExtractBool('{"passed": true}', 'passed');
SELECT JSONExtractBool('"HX-=');

SELECT '--JSONExtractString--';
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
select JSONExtractString('{"abc":"\\n\\u0000"}', 'abc');
select JSONExtractString('{"abc":"\\u263a"}', 'abc');
select JSONExtractString('{"abc":"\\u263"}', 'abc');
select JSONExtractString('{"abc":"hello}', 'abc');

SELECT '--JSONExtract (generic)--';
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(string, array(float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(a string, b array(float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(b array(float64), a string)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'tuple(a FixedString(6), c uint8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'a', 'string');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(Float32)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'tuple(int8, Float32, uint16)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(int8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(Nullable(int8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(uint8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'array(Nullable(uint8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1, 'int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2, 'int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(int64)');
SELECT JSONExtract('{"passed": true}', 'passed', 'uint8');
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(a int, b int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(c int, a int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(b int, d int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'tuple(int, int)');
SELECT JSONExtract('{"a":3}', 'tuple(int, int)');
SELECT JSONExtract('[3,5,7]', 'tuple(int, int)');
SELECT JSONExtract('[3]', 'tuple(int, int)');

SELECT '--JSONExtractKeysAndValues--';
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'string');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'array(float64)');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}', 'string');
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'int8');

SELECT '--JSONExtractRaw--';
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 2);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 3);
SELECT JSONExtractRaw('{"passed": true}');
SELECT JSONExtractRaw('{}');
SELECT JSONExtractRaw('{"abc":"\\n\\u0000"}', 'abc');
SELECT JSONExtractRaw('{"abc":"\\u263a"}', 'abc');

SELECT '--JSONExtractArrayRaw--';
SELECT JSONExtractArrayRaw('');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": "not_array"}');
SELECT JSONExtractArrayRaw('[]');
SELECT JSONExtractArrayRaw('[[],[]]');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractArrayRaw('[1,2,3,4,5,"hello"]');
SELECT JSONExtractArrayRaw(array_join(JSONExtractArrayRaw('[[1,2,3],[4,5,6]]')));

SELECT '--JSONExtractKeysAndValuesRaw--';
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');

SELECT '--JSONExtractKeys--';
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT array_join([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json, 's') FROM (SELECT array_join(['{"s":"u"}', '{"s":"v"}']) AS json);

SELECT '--show error: type should be const string';
SELECT JSONExtractKeysAndValues([], JSONLength('^?V{LSwp')); -- { serverError 44 }
WITH '{"i": 1, "f": 1.2}' AS json SELECT JSONExtract(json, 'i', JSONType(json, 'i')); -- { serverError 44 }
