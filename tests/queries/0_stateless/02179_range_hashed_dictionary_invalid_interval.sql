DROP STREAM IF EXISTS 02179_test_table;
create stream 02179_test_table
(
    id uint64,
    value string,
    start int64,
    end int64
) ;

INSERT INTO 02179_test_table VALUES (0, 'Value', 10, 0);
INSERT INTO 02179_test_table VALUES (0, 'Value', 15, 10);
INSERT INTO 02179_test_table VALUES (0, 'Value', 15, 20);

DROP DICTIONARY IF EXISTS 02179_test_dictionary;
CREATE DICTIONARY 02179_test_dictionary
(
    id uint64,
    value string DEFAULT 'DefaultValue',
    start int64,
    end int64
) PRIMARY KEY id
LAYOUT(RANGE_HASHED())
SOURCE(CLICKHOUSE(TABLE '02179_test_table'))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT dictGet('02179_test_dictionary', 'value', 0, 15);
SELECT dictGet('02179_test_dictionary', 'value', 0, 5);

SELECT dictHas('02179_test_dictionary', 0, 15);
SELECT dictHas('02179_test_dictionary', 0, 5);

SELECT * FROM 02179_test_dictionary;

DROP DICTIONARY 02179_test_dictionary;
DROP STREAM 02179_test_table;
