DROP STREAM IF EXISTS 02184_range_dictionary_source_table;
create stream 02184_range_dictionary_source_table
(
    id uint64,
    start uint64,
    end uint64,
    value_0 string,
    value_1 string,
    value_2 string
)
;

INSERT INTO 02184_range_dictionary_source_table VALUES (1, 0, 18446744073709551615, 'value0', 'value1', 'value2');

DROP DICTIONARY IF EXISTS 02184_range_dictionary;
CREATE DICTIONARY 02184_range_dictionary
(
    id uint64,
    start uint64,
    end uint64,
    value_0 string,
    value_1 string,
    value_2 string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02184_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT * FROM 02184_range_dictionary;
SELECT dictGet('02184_range_dictionary', ('value_0', 'value_1', 'value_2'), 1, 18446744073709551615);
SELECT dictHas('02184_range_dictionary', 1, 18446744073709551615);

DROP DICTIONARY 02184_range_dictionary;
DROP STREAM 02184_range_dictionary_source_table;
