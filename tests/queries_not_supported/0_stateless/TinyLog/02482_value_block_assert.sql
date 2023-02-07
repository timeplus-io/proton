SET allow_suspicious_low_cardinality_types=1;
CREATE STREAM range_key_dictionary_source_table__fuzz_323
(
    `key` uint256,
    `start_date` int8,
    `end_date` low_cardinality(uint256),
    `value` tuple(uint8, array(DateTime), decimal(9, 1), array(int16), array(uint8)),
    `value_nullable` uuid
)
ENGINE = TinyLog;
INSERT INTO range_key_dictionary_source_table__fuzz_323 FORMAT Values
(1, to_date('2019-05-20'), to_date('2019-05-20'), 'First', 'First'); -- { clientError CANNOT_PARSE_INPUT_ASSERTION_FAILED }


CREATE STREAM complex_key_dictionary_source_table__fuzz_267
(
    `id` decimal(38, 30),
    `id_key` array(uuid),
    `value` array(nullable(DateTime64(3))),
    `value_nullable` nullable(uuid)
)
ENGINE = TinyLog;
INSERT INTO complex_key_dictionary_source_table__fuzz_267 FORMAT Values
(1, 'key', 'First', 'First'); -- { clientError CANNOT_READ_ARRAY_FROM_TEXT }
