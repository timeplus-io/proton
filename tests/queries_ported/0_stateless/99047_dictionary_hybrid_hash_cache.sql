CREATE DATABASE IF NOT EXISTS test_99047;

DROP STREAM IF EXISTS test_99047.99047_left_stream;
DROP DICTIONARY IF EXISTS test_99047.99047_dict;
DROP STREAM IF EXISTS test_99047.99047_dict_source;

CREATE STREAM test_99047.99047_dict_source
(
    id string,
    name string
) ENGINE = MergeTree() ORDER BY (id, name);

CREATE STREAM test_99047.99047_left_stream
(
    id string,
    name string
) ENGINE = MergeTree() ORDER BY (id, name);

CREATE DICTIONARY test_99047.99047_dict(
    id string,
    name string
)
PRIMARY KEY id
SOURCE(TIMEPLUS(USER 'proton' PASSWORD 'proton@t+' DB 'test_99047' STREAM '99047_dict_source'))
LAYOUT(hybrid_hash_cache(TTL 3600 PATH 'test_99047_dict' max_hot_key_count 33));

show create test_99047.99047_dict;

INSERT INTO test_99047.99047_left_stream(id, name) VALUES ('1', 'a')('2', 'b');
select sleep(1) format Null;

SELECT 'Join empty cache - left';
SELECT * FROM test_99047.99047_left_stream LEFT JOIN test_99047.99047_dict ON 99047_left_stream.id = 99047_dict.id ORDER BY (99047_left_stream.id, 99047_left_stream.name) SETTINGS join_algorithm = 'direct';

SELECT 'Join empty cache - inner';
SELECT * FROM test_99047.99047_left_stream JOIN test_99047.99047_dict ON 99047_left_stream.id = 99047_dict.id ORDER BY (99047_left_stream.id, 99047_left_stream.name) SETTINGS join_algorithm = 'direct';

SELECT 'Join empty cache - anti';
SELECT * FROM test_99047.99047_left_stream ANTI JOIN test_99047.99047_dict ON 99047_left_stream.id = 99047_dict.id ORDER BY (99047_left_stream.id, 99047_left_stream.name) SETTINGS join_algorithm = 'direct';

INSERT INTO test_99047.99047_dict_source(id, name) VALUES('1', 'one') ('2', 'two') ('3', 'three');
select sleep(1) format Null;

INSERT INTO test_99047.99047_left_stream(id, name) VALUES('3', 'c') ('4', 'd') ('4', 'e') ('3', 'f');
select sleep(1) format Null;

SELECT 'Join hybrid hash cache - left';
SELECT * FROM test_99047.99047_left_stream LEFT JOIN test_99047.99047_dict ON 99047_left_stream.id = 99047_dict.id ORDER BY (99047_left_stream.id, 99047_left_stream.name) SETTINGS join_algorithm = 'direct';

SELECT 'Join hybrid hash cache - inner';
SELECT * FROM test_99047.99047_left_stream JOIN test_99047.99047_dict ON 99047_left_stream.id = 99047_dict.id ORDER BY (99047_left_stream.id, 99047_left_stream.name) SETTINGS join_algorithm = 'direct';

SELECT 'Join hybrid hash cache - anti';
SELECT * FROM test_99047.99047_left_stream ANTI JOIN test_99047.99047_dict ON 99047_left_stream.id = 99047_dict.id ORDER BY (99047_left_stream.id, 99047_left_stream.name) SETTINGS join_algorithm = 'direct';

