DROP DICTIONARY IF EXISTS dict;
DROP STREAM IF EXISTS s;
create stream s
(
   id uint64,
   value string
)
;

INSERT INTO s VALUES(1, 'OK');

CREATE DICTIONARY dict
(
   id uint64,
   value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME clickhouse_dictionary PORT tcpPort() DB currentDatabase()))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT dictGet('dict', 'value', to_uint64(1));

DROP DICTIONARY dict;
DROP STREAM s;
