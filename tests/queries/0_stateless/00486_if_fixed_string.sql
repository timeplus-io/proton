SELECT number % 2 ? 'hello' : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? 'hello' : materialize('world') FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : materialize('world') FROM system.numbers LIMIT 5;

SELECT number % 2 ? to_fixed_string('hello', 5) : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(to_fixed_string('hello', 5)) : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? to_fixed_string('hello', 5) : materialize('world') FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(to_fixed_string('hello', 5)) : materialize('world') FROM system.numbers LIMIT 5;

SELECT number % 2 ? 'hello' : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? 'hello' : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;

SELECT number % 2 ? to_fixed_string('hello', 5) : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(to_fixed_string('hello', 5)) : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? to_fixed_string('hello', 5) : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(to_fixed_string('hello', 5)) : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;
