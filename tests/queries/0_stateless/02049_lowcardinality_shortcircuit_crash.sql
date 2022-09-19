-- https://github.com/ClickHouse/ClickHouse/issues/30231
SELECT *
FROM (
      SELECT number,
             multiIf(
                     CAST(number < 4, 'uint8'), to_string(number),
                     CAST(number < 8, 'LowCardinality(uint8)'), to_string(number * 10),
                     CAST(number < 12, 'Nullable(uint8)'), to_string(number * 100),
                     CAST(number < 16, 'LowCardinality(Nullable(uint8))'), to_string(number * 1000),
                     to_string(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
     )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';

SELECT *
FROM (
      SELECT number,
             multiIf(
                     CAST(number < 4, 'uint8'), to_string(number),
                     CAST(number < 8, 'LowCardinality(uint8)'), to_string(number * 10),
                     CAST(NULL, 'Nullable(uint8)'), to_string(number * 100),
                     CAST(NULL, 'LowCardinality(Nullable(uint8))'), to_string(number * 1000),
                     to_string(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
         )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';

SELECT *
FROM (
      SELECT number,
             multiIf(
                     CAST(number < 4, 'uint8'), to_string(number),
                     CAST(number < 8, 'LowCardinality(uint8)'), to_string(number * 10)::LowCardinality(string),
                     CAST(number < 12, 'Nullable(uint8)'), to_string(number * 100)::Nullable(string),
                     CAST(number < 16, 'LowCardinality(Nullable(uint8))'), to_string(number * 1000)::LowCardinality(Nullable(string)),
                     to_string(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
         )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';
