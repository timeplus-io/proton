-- Tags: no-parallel

DROP STREAM IF EXISTS dictionary_primary_key_source_table;
create stream dictionary_primary_key_source_table
(
    identifier uint64,
    v uint64
) ;

INSERT INTO dictionary_primary_key_source_table VALUES (20, 1);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    identifier uint64,
    v uint64
)
PRIMARY KEY v
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_primary_key_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(FLAT());

SELECT * FROM flat_dictionary;

DROP DICTIONARY flat_dictionary;
DROP STREAM dictionary_primary_key_source_table;
