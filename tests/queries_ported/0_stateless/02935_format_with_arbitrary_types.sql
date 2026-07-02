
-- Tags: no-fasttest
-- no-fasttest: json type needs rapidjson library, geo types need s2 geometry

SET enable_json_type = 1;
SET allow_suspicious_low_cardinality_types=1;

SELECT '-- Const string + non-const arbitrary type';
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42 :: int8));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(43 :: int16));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(44 :: int32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(45 :: int64));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(46 :: int128));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(47 :: int256));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(48 :: uint8));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(49 :: uint16));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(50 :: uint32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(51 :: uint64));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(52 :: uint128));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(53 :: uint256));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42.42 :: float32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(43.43 :: float64));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(44.44 :: decimal(9,2)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(true :: bool));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(false :: bool));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: string));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: fixed_string(3)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: nullable(string)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: nullable(fixed_string(3))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: low_cardinality(string)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: low_cardinality(fixed_string(3))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: low_cardinality(nullable(string))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: low_cardinality(nullable(fixed_string(3)))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42 :: low_cardinality(nullable(uint32))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42 :: low_cardinality(uint32)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('fae310ca-d52a-4923-9e9b-02bf67f4b009' :: uuid));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2023-11-14' :: date));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2123-11-14' :: date32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2023-11-14 05:50:12' :: datetime('Europe/Amsterdam')));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2023-11-14 05:50:12.123' :: datetime64(3, 'Europe/Amsterdam')));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('hallo' :: enum('hallo' = 1)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(['foo', 'bar'] :: array(string)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('{"foo": "bar"}' :: JSON));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize((42, 'foo') :: tuple(int32, string)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(map_cast(42, 'foo') :: map(int32, string)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('122.233.64.201' :: ipv4));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2001:0001:130F:0002:0003:09C0:876A:130B' :: ipv6));

SELECT '-- Nested';
DROP STREAM IF EXISTS format_nested;
CREATE STREAM format_nested(attrs nested(k string, v string));
INSERT INTO format_nested(attrs.k, attrs.v) VALUES (['foo', 'bar'], ['qaz', 'qux']);
SELECT sleep(3);
SELECT format('The {0} to all questions is {1}.', attrs.k, attrs.v) FROM table(format_nested);
DROP STREAM format_nested;

SELECT '-- NULL arguments';
SELECT format('The {0} to all questions is {1}', NULL, NULL);
SELECT format('The {0} to all questions is {1}', NULL, materialize(NULL :: nullable(uint64)));
SELECT format('The {0} to all questions is {1}', materialize(NULL :: nullable(uint64)), materialize(NULL :: nullable(uint64)));
SELECT format('The {0} to all questions is {1}', 42, materialize(NULL :: nullable(uint64)));
SELECT format('The {0} to all questions is {1}', '42', materialize(NULL :: nullable(uint64)));
SELECT format('The {0} to all questions is {1}', 42, materialize(NULL :: nullable(uint64)), materialize(NULL :: nullable(uint64)));
SELECT format('The {0} to all questions is {1}', '42', materialize(NULL :: nullable(uint64)), materialize(NULL :: nullable(uint64)));

SELECT '-- Various arguments tests';
SELECT format('The {0} to all questions is {1}', materialize('Non-const'), materialize(' strings'));
SELECT format('The {0} to all questions is {1}', 'Two arguments ', 'test');
SELECT format('The {0} to all questions is {1} and {2}', 'Three ', 'arguments', ' test');
SELECT format('The {0} to all questions is {1} and {2}', materialize(3 :: int64), ' arguments test', ' with int type');
SELECT format('The {0} to all questions is {1}', materialize(42 :: int32), materialize(144 :: uint64));
SELECT format('The {0} to all questions is {1} and {2}', materialize(42 :: int32), materialize(144 :: uint64), materialize(255 :: uint32));
SELECT format('The {0} to all questions is {1}', 42, 144);
SELECT format('The {0} to all questions is {1} and {2}', 42, 144, 255);

SELECT '-- Single argument tests';
SELECT format('The answer to all questions is {0}.', 42);
SELECT format('The answer to all questions is {0}.', materialize(42));
SELECT format('The answer to all questions is {0}.', 'foo');
SELECT format('The answer to all questions is {0}.', materialize('foo'));
SELECT format('The answer to all questions is {0}.', NULL);
SELECT format('The answer to all questions is {0}.', materialize(NULL :: nullable(uint64)));
