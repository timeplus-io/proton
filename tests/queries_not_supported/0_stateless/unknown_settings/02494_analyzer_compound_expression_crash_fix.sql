SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table (
    fingerprint uint16,
    fields array(tuple(name array(uint32), value string))
) ENGINE = MergeTree
ORDER BY fingerprint;

INSERT INTO test_table VALUES (0, [[1]], ['1']);

SELECT fields.name FROM (SELECT fields.name FROM test_table);

SELECT fields.name, fields.value FROM (SELECT fields.name FROM test_table); -- { serverError 36 }

DROP STREAM IF EXISTS test_table;
