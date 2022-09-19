DROP STREAM IF EXISTS 02125_test_table;
create stream 02125_test_table
(
    id uint64,
    value Nullable(string)
)
;

INSERT INTO 02125_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02125_test_dictionary;
CREATE DICTIONARY 02125_test_dictionary
(
    id uint64,
    value Nullable(string)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02125_test_table'))
LAYOUT(DIRECT());

SELECT dictGet('02125_test_dictionary', 'value', to_uint64(0));
SELECT dictGetString('02125_test_dictionary', 'value', to_uint64(0)); --{serverError 53}
