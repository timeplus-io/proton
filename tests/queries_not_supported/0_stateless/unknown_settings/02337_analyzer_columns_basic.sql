-- Tags: no-parallel

SET allow_experimental_analyzer = 1;

-- Empty from section

SELECT 'Empty from section';

DESCRIBE (SELECT dummy);
SELECT dummy;

SELECT '--';

DESCRIBE (SELECT one.dummy);
SELECT one.dummy;

SELECT '--';

DESCRIBE (SELECT system.one.dummy);
SELECT system.one.dummy;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Table access without stream name qualification';

SELECT test_id FROM test_table; -- { serverError 47 }
SELECT test_id FROM test_unknown_table; -- { serverError 47 }

DESCRIBE (SELECT id FROM test_table);
SELECT id FROM test_table;

SELECT '--';

DESCRIBE (SELECT value FROM test_table);
SELECT value FROM test_table;

SELECT '--';

DESCRIBE (SELECT id, value FROM test_table);
SELECT id, value FROM test_table;

SELECT 'Table access with stream name qualification';

DESCRIBE (SELECT test_table.id FROM test_table);
SELECT test_table.id FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.value FROM test_table);
SELECT test_table.value FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM test_table);
SELECT test_table.id, test_table.value FROM test_table;

SELECT '--';

DESCRIBE (SELECT test.id, test.value FROM test_table AS test);
SELECT test.id, test.value FROM test_table AS test;

DROP STREAM test_table;

SELECT 'Table access with database and stream name qualification';

DROP DATABASE IF EXISTS 02337_db;
CREATE DATABASE 02337_db;

DROP STREAM IF EXISTS 02337_db.test_table;
CREATE STREAM 02337_db.test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO 02337_db.test_table VALUES (0, 'Value');

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM 02337_db.test_table);
SELECT test_table.id, test_table.value FROM 02337_db.test_table;

SELECT '--';

DESCRIBE (SELECT 02337_db.test_table.id, 02337_db.test_table.value FROM 02337_db.test_table);
SELECT 02337_db.test_table.id, 02337_db.test_table.value FROM 02337_db.test_table;

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM 02337_db.test_table AS test_table);
SELECT test_table.id, test_table.value FROM 02337_db.test_table AS test_table;

DROP STREAM 02337_db.test_table;
DROP DATABASE 02337_db;
