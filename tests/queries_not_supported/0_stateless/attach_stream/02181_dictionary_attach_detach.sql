DROP STREAM IF EXISTS 02181_test_table;
create stream 02181_test_table
(
    id uint64,
    value string
)
;

INSERT INTO 02181_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02181_test_dictionary;
CREATE DICTIONARY 02181_test_dictionary
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02181_test_table'))
LAYOUT(HASHED())
LIFETIME(0);

DETACH STREAM 02181_test_dictionary; --{serverError 520}
ATTACH STREAM 02181_test_dictionary; --{serverError 80}

DETACH DICTIONARY 02181_test_dictionary;
ATTACH DICTIONARY 02181_test_dictionary;

SELECT * FROM 02181_test_dictionary;

DETACH DICTIONARY 02181_test_dictionary;
ATTACH DICTIONARY 02181_test_dictionary;

SELECT * FROM 02181_test_dictionary;

DETACH DICTIONARY 02181_test_dictionary;
ATTACH DICTIONARY 02181_test_dictionary;

DROP DICTIONARY 02181_test_dictionary;
DROP STREAM 02181_test_table;
