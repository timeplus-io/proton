DROP STREAM IF EXISTS 02186_range_dictionary_source_table;
create stream 02186_range_dictionary_source_table
(
    id uint64,
    start date,
    end date,
    value string
)
;

INSERT INTO 02186_range_dictionary_source_table VALUES (1, '2020-01-01', '2100-01-01', 'Value0');
INSERT INTO 02186_range_dictionary_source_table VALUES (1, '2020-01-02', '2100-01-01', 'Value1');
INSERT INTO 02186_range_dictionary_source_table VALUES (1, '2020-01-03', '2100-01-01', 'Value2');

SELECT 'Source table';
SELECT * FROM 02186_range_dictionary_source_table;

DROP DICTIONARY IF EXISTS 02186_range_dictionary;
CREATE DICTIONARY 02186_range_dictionary
(
    id uint64,
    start date,
    end date,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02186_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'min'))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'Dictionary .range_lookup_strategy = min';

SELECT * FROM 02186_range_dictionary;

select dictGet('02186_range_dictionary', 'value', to_uint64(1), to_date('2020-01-01'));
select dictGet('02186_range_dictionary', 'value', to_uint64(1), to_date('2020-01-02'));
select dictGet('02186_range_dictionary', 'value', to_uint64(1), to_date('2020-01-03'));

DROP DICTIONARY 02186_range_dictionary;

CREATE DICTIONARY 02186_range_dictionary
(
    id uint64,
    start date,
    end date,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02186_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'Dictionary .range_lookup_strategy = max';

SELECT * FROM 02186_range_dictionary;

select dictGet('02186_range_dictionary', 'value', to_uint64(1), to_date('2020-01-01'));
select dictGet('02186_range_dictionary', 'value', to_uint64(1), to_date('2020-01-02'));
select dictGet('02186_range_dictionary', 'value', to_uint64(1), to_date('2020-01-03'));

DROP DICTIONARY 02186_range_dictionary;
DROP STREAM 02186_range_dictionary_source_table;
