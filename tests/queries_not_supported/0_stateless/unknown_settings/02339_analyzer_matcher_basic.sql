-- Tags: no-parallel

SET allow_experimental_analyzer = 1;

SELECT 'Matchers without FROM section';

DESCRIBE (SELECT *);
SELECT *;

SELECT '--';

DESCRIBE (SELECT COLUMNS(dummy));
SELECT COLUMNS(dummy);

SELECT '--';

DESCRIBE (SELECT COLUMNS('d'));
SELECT COLUMNS('d');

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Unqualified matchers';

DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id) FROM test_table);
SELECT COLUMNS(id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id), COLUMNS(value) FROM test_table);
SELECT COLUMNS(id), COLUMNS(value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS('i'), COLUMNS('v') FROM test_table);
SELECT COLUMNS('i'), COLUMNS('v') FROM test_table;

SELECT 'Table qualified matchers';

DESCRIBE (SELECT test_table.* FROM test_table);
SELECT test_table.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.COLUMNS(id) FROM test_table);
SELECT test_table.COLUMNS(id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.COLUMNS(id), test_table.COLUMNS(value) FROM test_table);
SELECT test_table.COLUMNS(id), test_table.COLUMNS(value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.COLUMNS('i'), test_table.COLUMNS('v') FROM test_table);
SELECT test_table.COLUMNS('i'), test_table.COLUMNS('v') FROM test_table;

SELECT 'Database and stream qualified matchers';

DROP DATABASE IF EXISTS 02339_db;
CREATE DATABASE 02339_db;

DROP STREAM IF EXISTS 02339_db.test_table;
CREATE STREAM 02339_db.test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO 02339_db.test_table VALUES (0, 'Value');

-- TODO: Qualified COLUMNS where identifier has more than 2 parts are not supported on parser level

-- SELECT '--';

-- DESCRIBE (SELECT 02339_db.test_table.* FROM 02339_db.test_table);
-- SELECT 02339_db.test_table.* FROM 02339_db.test_table;

-- SELECT '--';

-- DESCRIBE (SELECT 02339_db.test_table.COLUMNS(id) FROM 02339_db.test_table);
-- SELECT 02339_db.test_table.COLUMNS(id) FROM 02339_db.test_table;

-- SELECT '--';

-- DESCRIBE (SELECT 02339_db.test_table.COLUMNS(id), 02339_db.test_table.COLUMNS(value) FROM 02339_db.test_table);
-- SELECT 02339_db.test_table.COLUMNS(id), 02339_db.test_table.COLUMNS(value) FROM 02339_db.test_table;

-- SELECT '--';

-- DESCRIBE (SELECT 02339_db.test_table.COLUMNS('i'), 02339_db.test_table.COLUMNS('v') FROM 02339_db.test_table);
-- SELECT 02339_db.test_table.COLUMNS('i'), 02339_db.test_table.COLUMNS('v') FROM 02339_db.test_table;

DROP STREAM 02339_db.test_table;
DROP DATABASE 02339_db;

SELECT 'APPLY transformer';

SELECT '--';

DESCRIBE (SELECT * APPLY to_string FROM test_table);
SELECT * APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT * APPLY (x -> to_string(x)) FROM test_table);
SELECT * APPLY (x -> to_string(x)) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * APPLY (x -> to_string(x)) APPLY (x -> length(x)) FROM test_table);
SELECT * APPLY (x -> to_string(x)) APPLY (x -> length(x)) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * APPLY (x -> to_string(x)) APPLY length FROM test_table);
SELECT * APPLY (x -> to_string(x)) APPLY length FROM test_table;

SELECT '--';
DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT 'EXCEPT transformer';

SELECT '--';

DESCRIBE (SELECT * EXCEPT (id) FROM test_table);
SELECT * EXCEPT (id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) EXCEPT (id) FROM test_table);
SELECT COLUMNS(id, value) EXCEPT (id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * EXCEPT (id) APPLY to_string FROM test_table);
SELECT * EXCEPT (id) APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) EXCEPT (id) APPLY to_string FROM test_table);
SELECT COLUMNS(id, value) EXCEPT (id) APPLY to_string FROM test_table;

SELECT 'REPLACE transformer';

SELECT '--';

DESCRIBE (SELECT * REPLACE (5 AS id) FROM test_table);
SELECT * REPLACE (5 AS id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) REPLACE (5 AS id) FROM test_table);
SELECT COLUMNS(id, value) REPLACE (5 AS id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * REPLACE (5 AS id, 6 as value) FROM test_table);
SELECT * REPLACE (5 AS id, 6 as value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) REPLACE (5 AS id, 6 as value) FROM test_table);
SELECT COLUMNS(id, value) REPLACE (5 AS id, 6 as value) FROM test_table;

SELECT 'Combine EXCEPT, REPLACE, APPLY transformers';

SELECT '--';

DESCRIBE (SELECT * EXCEPT id REPLACE (5 AS id, 6 as value) APPLY to_string FROM test_table);
SELECT * EXCEPT id REPLACE (5 AS id, 6 as value) APPLY to_string FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) EXCEPT id REPLACE (5 AS id, 6 as value) APPLY to_string FROM test_table);
SELECT COLUMNS(id, value) EXCEPT id REPLACE (5 AS id, 6 as value) APPLY to_string FROM test_table;
