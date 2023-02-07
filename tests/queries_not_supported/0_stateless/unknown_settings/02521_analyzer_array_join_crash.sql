SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value');

-- { echoOn }

SELECT id, value_element, value FROM test_table ARRAY JOIN [[1,2,3]] AS value_element, value_element AS value;

SELECT id, value_element, value FROM test_table ARRAY JOIN [[1,2,3]] AS value_element ARRAY JOIN value_element AS value;

SELECT value_element, value FROM test_table ARRAY JOIN [1048577] AS value_element, array_map(x -> value_element, ['']) AS value;

SELECT arrayFilter(x -> notEmpty(concat(x)), [NULL, NULL]) FROM system.one ARRAY JOIN [1048577] AS elem, array_map(x -> concat(x, elem, ''), ['']) AS unused; -- { serverError 44 }

-- { echoOff }

DROP STREAM test_table;
