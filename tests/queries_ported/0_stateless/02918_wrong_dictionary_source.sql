DROP DICTIONARY IF EXISTS id_value_dictionary;
DROP STREAM IF EXISTS source_table;

CREATE STREAM source_table(id uint64, value string) ENGINE = Memory;

-- There is no "CLICKHOUSEX" dictionary source, so the next query must fail even if `dictionaries_lazy_load` is enabled.
CREATE DICTIONARY id_value_dictionary(id uint64, value string) PRIMARY KEY id SOURCE(CLICKHOUSEX(TABLE 'source_table')) LIFETIME(MIN 0 MAX 1000) LAYOUT(FLAT()); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

SELECT count() FROM system.dictionaries WHERE name=='id_value_dictionary' AND database==current_database();

DROP STREAM source_table;
