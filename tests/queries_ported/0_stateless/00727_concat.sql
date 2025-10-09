-- Tags: no-fasttest
-- no-fasttest: json type needs rapidjson library, geo types need s2 geometry

SET enable_json_type = 1;
SET allow_suspicious_low_cardinality_types=1;

SELECT '-- Const string + non-const arbitrary type';
SELECT concat('With ', materialize(42 :: int8));
SELECT concat('With ', materialize(43 :: int16));
SELECT concat('With ', materialize(44 :: int32));
SELECT concat('With ', materialize(45 :: int64));
SELECT concat('With ', materialize(46 :: int128));
SELECT concat('With ', materialize(47 :: int256));
SELECT concat('With ', materialize(48 :: uint8));
SELECT concat('With ', materialize(49 :: uint16));
SELECT concat('With ', materialize(50 :: uint32));
SELECT concat('With ', materialize(51 :: uint64));
SELECT concat('With ', materialize(52 :: uint128));
SELECT concat('With ', materialize(53 :: uint256));
SELECT concat('With ', materialize(42.42 :: float32));
SELECT concat('With ', materialize(43.43 :: float64));
SELECT concat('With ', materialize(44.44 :: Decimal(2, 0)));
SELECT concat('With ', materialize(true :: bool));
SELECT concat('With ', materialize(false :: bool));
SELECT concat('With ', materialize('foo' :: string));
SELECT concat('With ', materialize('bar' :: fixed_string(3)));
SELECT concat('With ', materialize('foo' :: nullable(string)));
SELECT concat('With ', materialize('bar' :: nullable(fixed_string(3))));
SELECT concat('With ', materialize('foo' :: low_cardinality(string)));
SELECT concat('With ', materialize('bar' :: low_cardinality(fixed_string(3))));
SELECT concat('With ', materialize('foo' :: low_cardinality(nullable(string))));
SELECT concat('With ', materialize('bar' :: low_cardinality(nullable(fixed_string(3)))));
SELECT concat('With ', materialize(42 :: low_cardinality(nullable(uint32))));
SELECT concat('With ', materialize(42 :: low_cardinality(uint32)));
SELECT concat('With ', materialize('fae310ca-d52a-4923-9e9b-02bf67f4b009' :: uuid));
SELECT concat('With ', materialize('2023-11-14' :: Date));
SELECT concat('With ', materialize('2123-11-14' :: Date32));
SELECT concat('With ', materialize('2023-11-14 05:50:12' :: DateTime('Europe/Amsterdam')));
SELECT concat('With ', materialize('2023-11-14 05:50:12.123' :: DateTime64(3, 'Europe/Amsterdam')));
SELECT concat('With ', materialize('hallo' :: enum('hallo' = 1)));
SELECT concat('With ', materialize(['foo', 'bar'] :: array(string)));
SELECT concat('With ', materialize('{"foo": "bar"}' :: JSON));
-- SELECT concat('With ', materialize('{"foo": "bar"}' :: object('json')));
SELECT concat('With ', materialize((42, 'foo') :: tuple(int32, string)));
SELECT concat('With ', materialize(map_cast(42, 'foo') :: map(int32, string)));
SELECT concat('With ', materialize('122.233.64.201' :: IPv4));
SELECT concat('With ', materialize('2001:0001:130F:0002:0003:09C0:876A:130B' :: IPv6));
-- SELECT concat('With ', materialize((42, 43) :: Point));
-- SELECT concat('With ', materialize([(0,0),(10,0),(10,10),(0,10)] :: Ring));
-- SELECT concat('With ', materialize([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]] :: Polygon));
-- SELECT concat('With ', materialize([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]] :: MultiPolygon));

SELECT '-- SimpleAggregateFunction';
DROP STREAM IF EXISTS concat_saf_test;
CREATE STREAM concat_saf_test(x simple_aggregate_function(max, int32)) ENGINE=MergeTree ORDER BY tuple_cast();
INSERT INTO concat_saf_test VALUES (42);
INSERT INTO concat_saf_test SELECT max(number) FROM numbers(5);
SELECT concat('With ', x) FROM concat_saf_test ORDER BY x DESC;
DROP STREAM concat_saf_test;

SELECT '-- Nested';
DROP STREAM IF EXISTS concat_nested_test;
CREATE STREAM concat_nested_test(attrs nested(k string, v string)) ENGINE = MergeTree ORDER BY tuple_cast();
INSERT INTO concat_nested_test VALUES (['foo', 'bar'], ['qaz', 'qux']);
SELECT concat('With ', attrs.k, attrs.v) FROM concat_nested_test;
DROP STREAM concat_nested_test;

SELECT '-- NULL arguments';
SELECT concat(NULL, NULL);
SELECT concat(NULL, materialize(NULL :: nullable(uint64)));
SELECT concat(materialize(NULL :: nullable(uint64)), materialize(NULL :: nullable(uint64)));
SELECT concat(42, materialize(NULL :: nullable(uint64)));
SELECT concat('42', materialize(NULL :: nullable(uint64)));
SELECT concat(42, materialize(NULL :: nullable(uint64)), materialize(NULL :: nullable(uint64)));
SELECT concat('42', materialize(NULL :: nullable(uint64)), materialize(NULL :: nullable(uint64)));

SELECT '-- Various arguments tests';
SELECT concat(materialize('Non-const'), materialize(' strings'));
SELECT concat('Two arguments ', 'test');
SELECT concat('Three ', 'arguments', ' test');
SELECT concat(materialize(3 :: int64), ' arguments test', ' with int type');
SELECT concat(materialize(42 :: int32), materialize(144 :: uint64));
SELECT concat(materialize(42 :: int32), materialize(144 :: uint64), materialize(255 :: uint32));
SELECT concat(42, 144);
SELECT concat(42, 144, 255);

SELECT '-- Single argument tests';
SELECT concat(42);
SELECT concat(materialize(42));
SELECT concat('foo');
SELECT concat(materialize('foo'));
SELECT concat(NULL);
SELECT concat(materialize(NULL :: nullable(uint64)));

SELECT CONCAT('Testing the ', 'alias');

SELECT '-- Empty argument tests';
SELECT concat();
select to_type_name(concat());
