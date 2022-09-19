DROP STREAM IF EXISTS nullable_00457;

create stream nullable_00457 (s string, ns Nullable(string), narr array(Nullable(uint64)))  ;

INSERT INTO nullable_00457 SELECT to_string(number), number % 3 = 1 ? to_string(number) : NULL, array_map(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable_00457 ORDER BY s;
SELECT s FROM nullable_00457 ORDER BY s;
SELECT ns FROM nullable_00457 ORDER BY s;
SELECT narr FROM nullable_00457 ORDER BY s;
SELECT s, narr FROM nullable_00457 ORDER BY s;

INSERT INTO nullable_00457 SELECT to_string(number), number % 3 = 1 ? to_string(number) : NULL, array_map(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP STREAM IF EXISTS nullable_00457;

create stream nullable_00457 (s string, ns Nullable(string), narr array(Nullable(uint64))) ;

INSERT INTO nullable_00457 SELECT to_string(number), number % 3 = 1 ? to_string(number) : NULL, array_map(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable_00457 ORDER BY s;
SELECT s FROM nullable_00457 ORDER BY s;
SELECT ns FROM nullable_00457 ORDER BY s;
SELECT narr FROM nullable_00457 ORDER BY s;
SELECT s, narr FROM nullable_00457 ORDER BY s;

INSERT INTO nullable_00457 SELECT to_string(number), number % 3 = 1 ? to_string(number) : NULL, array_map(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP STREAM IF EXISTS nullable_00457;

create stream nullable_00457 (s string, ns Nullable(string), narr array(Nullable(uint64))) ENGINE = StripeLog;

INSERT INTO nullable_00457 SELECT to_string(number), number % 3 = 1 ? to_string(number) : NULL, array_map(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable_00457 ORDER BY s;
SELECT s FROM nullable_00457 ORDER BY s;
SELECT ns FROM nullable_00457 ORDER BY s;
SELECT narr FROM nullable_00457 ORDER BY s;
SELECT s, narr FROM nullable_00457 ORDER BY s;

INSERT INTO nullable_00457 SELECT to_string(number), number % 3 = 1 ? to_string(number) : NULL, array_map(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP STREAM nullable_00457;
