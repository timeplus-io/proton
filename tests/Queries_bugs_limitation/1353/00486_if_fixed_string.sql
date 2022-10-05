SELECT (number % 2 <> 0) ? 'hello' : 'world' FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize('hello') : 'world' FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? 'hello' : materialize('world') FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize('hello') : materialize('world') FROM system.numbers LIMIT 5;

SELECT (number % 2 <> 0) ? to_fixed_string('hello', 5) : 'world' FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize(to_fixed_string('hello', 5)) : 'world' FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? to_fixed_string('hello', 5) : materialize('world') FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize(to_fixed_string('hello', 5)) : materialize('world') FROM system.numbers LIMIT 5;

SELECT (number % 2 <> 0) ? 'hello' : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize('hello') : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? 'hello' : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize('hello') : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;

SELECT (number % 2 <> 0) ? to_fixed_string('hello', 5) : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize(to_fixed_string('hello', 5)) : to_fixed_string('world', 5) FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? to_fixed_string('hello', 5) : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;
SELECT (number % 2 <> 0) ? materialize(to_fixed_string('hello', 5)) : materialize(to_fixed_string('world', 5)) FROM system.numbers LIMIT 5;
