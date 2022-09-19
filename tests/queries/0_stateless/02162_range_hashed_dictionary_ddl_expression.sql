DROP STREAM IF EXISTS 02162_test_table;
create stream 02162_test_table
(
    id uint64,
    value string,
    range_value uint64
) ;

INSERT INTO 02162_test_table VALUES (0, 'Value', 1);

DROP DICTIONARY IF EXISTS 02162_test_dictionary;
CREATE DICTIONARY 02162_test_dictionary
(
    id uint64,
    value string,
    range_value uint64,
    start uint64 EXPRESSION range_value,
    end uint64 EXPRESSION range_value
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02162_test_table'))
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT * FROM 02162_test_dictionary;

DROP DICTIONARY 02162_test_dictionary;
DROP STREAM 02162_test_table;
