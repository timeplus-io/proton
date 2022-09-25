DROP STREAM IF EXISTS 02185_range_dictionary_source_table;
create stream 02185_range_dictionary_source_table
(
    id uint64,
    start nullable(uint64),
    end nullable(uint64),
    value string
)
;

INSERT INTO 02185_range_dictionary_source_table VALUES (0, NULL, 5000, 'Value0'), (0, 5001, 10000, 'Value1'), (0, 10001, NULL, 'Value2');

SELECT 'Source table';
SELECT * FROM 02185_range_dictionary_source_table;

DROP DICTIONARY IF EXISTS 02185_range_dictionary;
CREATE DICTIONARY 02185_range_dictionary
(
    id uint64,
    start uint64,
    end uint64,
    value string DEFAULT 'DefaultValue'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02185_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED(convert_null_range_bound_to_open 1))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'Dictionary convert_null_range_bound_to_open = 1';
SELECT * FROM 02185_range_dictionary;
SELECT dictGet('02185_range_dictionary', 'value', 0, 0);
SELECT dictGet('02185_range_dictionary', 'value', 0, 5001);
SELECT dictGet('02185_range_dictionary', 'value', 0, 10001);
SELECT dictHas('02185_range_dictionary', 0, 0);
SELECT dictHas('02185_range_dictionary', 0, 5001);
SELECT dictHas('02185_range_dictionary', 0, 10001);

DROP DICTIONARY 02185_range_dictionary;

CREATE DICTIONARY 02185_range_dictionary
(
    id uint64,
    start uint64,
    end uint64,
    value string DEFAULT 'DefaultValue'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02185_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED(convert_null_range_bound_to_open 0))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'Dictionary convert_null_range_bound_to_open = 0';
SELECT * FROM 02185_range_dictionary;
SELECT dictGet('02185_range_dictionary', 'value', 0, 0);
SELECT dictGet('02185_range_dictionary', 'value', 0, 5001);
SELECT dictGet('02185_range_dictionary', 'value', 0, 10001);
SELECT dictHas('02185_range_dictionary', 0, 0);
SELECT dictHas('02185_range_dictionary', 0, 5001);
SELECT dictHas('02185_range_dictionary', 0, 10001);

DROP STREAM 02185_range_dictionary_source_table;
