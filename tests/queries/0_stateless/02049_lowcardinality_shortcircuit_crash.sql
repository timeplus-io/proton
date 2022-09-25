-- https://github.com/ClickHouse/ClickHouse/issues/30231
SELECT *
FROM (
      SELECT number,
             multi_if(
                     CAST(number < 4, 'uint8'), to_string(number),
                     CAST(number < 8, 'low_cardinality(uint8)'), to_string(number * 10),
                     CAST(number < 12, 'nullable(uint8)'), to_string(number * 100),
                     CAST(number < 16, 'low_cardinality(nullable(uint8))'), to_string(number * 1000),
                     to_string(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
     )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';

SELECT *
FROM (
      SELECT number,
             multi_if(
                     CAST(number < 4, 'uint8'), to_string(number),
                     CAST(number < 8, 'low_cardinality(uint8)'), to_string(number * 10),
                     CAST(NULL, 'nullable(uint8)'), to_string(number * 100),
                     CAST(NULL, 'low_cardinality(nullable(uint8))'), to_string(number * 1000),
                     to_string(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
         )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';

SELECT *
FROM (
      SELECT number,
             multi_if(
                     CAST(number < 4, 'uint8'), to_string(number),
                     CAST(number < 8, 'low_cardinality(uint8)'), to_string(number * 10)::low_cardinality(string),
                     CAST(number < 12, 'nullable(uint8)'), to_string(number * 100)::nullable(string),
                     CAST(number < 16, 'low_cardinality(nullable(uint8))'), to_string(number * 1000)::low_cardinality(nullable(string)),
                     to_string(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
         )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';
