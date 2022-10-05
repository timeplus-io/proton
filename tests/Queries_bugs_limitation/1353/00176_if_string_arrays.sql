SELECT (number % 2 <> 0) ? ['Hello', 'World'] : ['abc'] FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? materialize(['Hello', 'World']) : ['abc'] FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? ['Hello', 'World'] : materialize(['abc']) FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? materialize(['Hello', 'World']) : materialize(['abc']) FROM system.numbers LIMIT 10;

SELECT (number % 2 <> 0) ? ['Hello', '', 'World!'] : empty_array_string() FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? materialize(['Hello', '', 'World!']) : empty_array_string() FROM system.numbers LIMIT 10;

SELECT (number % 2 <> 0) ? [''] : ['', ''] FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? materialize(['']) : ['', ''] FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? [''] : materialize(['', '']) FROM system.numbers LIMIT 10;
SELECT (number % 2 <> 0) ? materialize(['']) : materialize(['', '']) FROM system.numbers LIMIT 10;
