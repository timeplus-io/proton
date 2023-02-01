-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS database_for_dict;
CREATE DATABASE database_for_dict;

DROP STREAM IF EXISTS database_for_dict.dict_source;
CREATE STREAM database_for_dict.dict_source (id uint64, parent_id uint64, value string) ENGINE = Memory;
INSERT INTO database_for_dict.dict_source VALUES (1, 0, 'hello'), (2, 1, 'world'), (3, 2, 'upyachka'), (11, 22, 'a'), (22, 11, 'b');

DROP DICTIONARY IF EXISTS database_for_dict.dictionary_with_hierarchy;

CREATE DICTIONARY database_for_dict.dictionary_with_hierarchy
(
    id uint64, parent_id uint64 HIERARCHICAL, value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(host 'localhost' port tcp_port() user 'default' db 'database_for_dict' stream 'dict_source'))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1);

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(2), to_uint64(1));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(22), to_uint64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), to_uint64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(11), materialize(to_uint64(22)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), materialize(to_uint64(11)));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(22), to_uint64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), to_uint64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(11), materialize(to_uint64(222)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), materialize(to_uint64(111)));

SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(11));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(22));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(11)));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)));


DROP DICTIONARY IF EXISTS database_for_dict.dictionary_with_hierarchy;

CREATE DICTIONARY database_for_dict.dictionary_with_hierarchy
(
    id uint64, parent_id uint64 HIERARCHICAL, value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(host 'localhost' port tcp_port() user 'default' db 'database_for_dict' stream 'dict_source'))
LAYOUT(FLAT())
LIFETIME(MIN 1 MAX 1);

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(2), to_uint64(1));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(22), to_uint64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), to_uint64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(11), materialize(to_uint64(22)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), materialize(to_uint64(11)));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(22), to_uint64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), to_uint64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(11), materialize(to_uint64(222)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), materialize(to_uint64(111)));

SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(11));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(22));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(11)));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)));


DROP DICTIONARY IF EXISTS database_for_dict.dictionary_with_hierarchy;

CREATE DICTIONARY database_for_dict.dictionary_with_hierarchy
(
    id uint64, parent_id uint64 HIERARCHICAL, value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(host 'localhost' port tcp_port() user 'default' db 'database_for_dict' stream 'dict_source'))
LAYOUT(CACHE(SIZE_IN_CELLS 10))
LIFETIME(MIN 1 MAX 1);

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(2), to_uint64(1));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(22), to_uint64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), to_uint64(11));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(11), materialize(to_uint64(22)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), materialize(to_uint64(11)));

SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(22), to_uint64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), to_uint64(111));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(11), materialize(to_uint64(222)));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)), materialize(to_uint64(111)));

SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(11));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(22));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(11)));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', materialize(to_uint64(22)));


DROP DICTIONARY database_for_dict.dictionary_with_hierarchy;
DROP STREAM database_for_dict.dict_source;
DROP DATABASE database_for_dict;
