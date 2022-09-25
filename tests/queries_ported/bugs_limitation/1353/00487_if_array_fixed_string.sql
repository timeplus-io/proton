SELECT (number % 2 <> 0) ? array_map(x -> to_fixed_string(x, 5), ['hello', 'world']) : array_map(x -> to_fixed_string(x, 5), ['a', 'b', 'c']) FROM system.numbers LIMIT 4;
SELECT (number % 2 <> 0) ? materialize(array_map(x -> to_fixed_string(x, 5), ['hello', 'world'])) : array_map(x -> to_fixed_string(x, 5), ['a', 'b', 'c']) FROM system.numbers LIMIT 4;
SELECT (number % 2 <> 0) ? array_map(x -> to_fixed_string(x, 5), ['hello', 'world']) : materialize(array_map(x -> to_fixed_string(x, 5), ['a', 'b', 'c'])) FROM system.numbers LIMIT 4;
SELECT (number % 2 <> 0) ? materialize(array_map(x -> to_fixed_string(x, 5), ['hello', 'world'])) : materialize(array_map(x -> to_fixed_string(x, 5), ['a', 'b', 'c'])) FROM system.numbers LIMIT 4;
