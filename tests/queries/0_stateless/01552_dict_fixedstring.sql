DROP STREAM IF EXISTS src;

create stream src (k uint64, s FixedString(11)) ;
INSERT INTO src VALUES (1, 'Hello\0World');

DROP DICTIONARY IF EXISTS dict;
CREATE DICTIONARY dict
(
    k uint64,
    s string
)
PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER default TABLE 'src'))
LAYOUT(FLAT)
LIFETIME(MIN 10 MAX 10);

SELECT dictGet(currentDatabase() || '.dict', 's', number) FROM numbers(2);

DROP STREAM src;
DROP DICTIONARY dict;
