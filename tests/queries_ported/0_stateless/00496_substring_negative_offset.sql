SELECT substring('abc', number - 5) FROM system.numbers LIMIT 10;
SELECT substring(materialize('abc'), number - 5) FROM system.numbers LIMIT 10;
SELECT substring(to_fixed_string('abc', 3), number - 5) FROM system.numbers LIMIT 10;
SELECT substring(materialize(to_fixed_string('abc', 3)), number - 5) FROM system.numbers LIMIT 10;
