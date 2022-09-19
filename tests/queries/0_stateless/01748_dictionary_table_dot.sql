-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01748;
CREATE DATABASE test_01748;
USE test_01748;

DROP STREAM IF EXISTS `test.txt`;
DROP DICTIONARY IF EXISTS test_dict;

create stream `test.txt`
(
    `key1` uint32,
    `key2` uint32,
    `value` string
)
();

CREATE DICTIONARY test_dict
(
    `key1` uint32,
    `key2` uint32,
    `value` string
)
PRIMARY KEY key1, key2
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE `test.txt` PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 1 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

INSERT INTO `test.txt` VALUES (1, 2, 'Hello');

-- TODO: it does not work without fully qualified name.
SYSTEM RELOAD DICTIONARY test_01748.test_dict;

SELECT dictGet(test_dict, 'value', (to_uint32(1), to_uint32(2)));

DROP DATABASE test_01748;
