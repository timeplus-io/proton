SET allow_experimental_analyzer = 1;

SELECT 'Constant tuple';

SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, value.id, value.value;
SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, value.* APPLY to_string;
SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, value.COLUMNS(id) APPLY to_string;
SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, value.COLUMNS(value) APPLY to_string;
SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, value.COLUMNS('i') APPLY to_string;
SELECT cast((1, 'Value'), 'Tuple (id uint64, value string)') AS value, value.COLUMNS('v') APPLY to_string;

SELECT 'Tuple';

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value Tuple(value_0_level_0 Tuple(value_0_level_1 string, value_1_level_1 string), value_1_level_0 string)
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, (('value_0_level_1', 'value_1_level_1'), 'value_1_level_0'));

SELECT '--';

DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT '--';

DESCRIBE (SELECT id, value FROM test_table);
SELECT id, value FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table);
SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value AS alias_value, alias_value.value_0_level_0, alias_value.value_1_level_0 FROM test_table);
SELECT value AS alias_value, alias_value.value_0_level_0, alias_value.value_1_level_0 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value AS alias_value, alias_value.* FROM test_table);
SELECT value AS alias_value, alias_value.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value AS alias_value, alias_value.* APPLY to_string FROM test_table);
SELECT value AS alias_value, alias_value.* APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.* FROM test_table);
SELECT value.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.* APPLY to_string FROM test_table);
SELECT value.* APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table);
SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY to_string FROM test_table);
SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* FROM test_table);
SELECT value.value_0_level_0.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* APPLY to_string FROM test_table);
SELECT value.value_0_level_0.* APPLY to_string FROM test_table;

DROP STREAM test_table;

-- SELECT 'array of tuples';

-- DROP STREAM IF EXISTS test_table;
-- CREATE STREAM test_table
-- (
--     id uint64,
--     value array(Tuple(value_0_level_0 Tuple(value_0_level_1 string, value_1_level_1 string), value_1_level_0 string))
-- ) ENGINE=MergeTree ORDER BY id;

-- INSERT INTO test_table VALUES (0, [('value_0_level_1', 'value_1_level_1')], ['value_1_level_0']);

-- DESCRIBE (SELECT * FROM test_table);
-- SELECT * FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table);
-- SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table);
-- SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table);
-- SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table);
-- SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY to_string FROM test_table);
-- SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY to_string FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0.* FROM test_table);
-- SELECT value.value_0_level_0.* FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0.* APPLY to_string FROM test_table);
-- SELECT value.value_0_level_0.* APPLY to_string FROM test_table;

-- DROP STREAM test_table;

SELECT 'Nested';

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value Nested (value_0_level_0 Nested(value_0_level_1 string, value_1_level_1 string), value_1_level_0 string)
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, [[('value_0_level_1', 'value_1_level_1')]], ['value_1_level_0']);

DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table);
SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS value_alias, value_alias.value_0_level_1, value_alias.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0 AS value_alias, value_alias.value_0_level_1, value_alias.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS value_alias, value_alias.* FROM test_table);
SELECT value.value_0_level_0 AS value_alias, value_alias.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS value_alias, value_alias.* APPLY to_string FROM test_table);
SELECT value.value_0_level_0 AS value_alias, value_alias.* APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* FROM test_table);
SELECT value.value_0_level_0.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* APPLY to_string FROM test_table);
SELECT value.value_0_level_0.* APPLY to_string FROM test_table;

DROP STREAM test_table;
