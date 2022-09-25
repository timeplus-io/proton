-- Tags: no-parallel

DROP DATABASE IF EXISTS 01765_db;
CREATE DATABASE 01765_db;

create stream 01765_db.simple_key_simple_attributes_source_table
(
   id uint64,
   value_first string,
   value_second string
)
;

INSERT INTO 01765_db.simple_key_simple_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO 01765_db.simple_key_simple_attributes_source_table VALUES(1, 'value_1', 'value_second_1');
INSERT INTO 01765_db.simple_key_simple_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

CREATE DICTIONARY 01765_db.hashed_dictionary_simple_key_simple_attributes
(
   id uint64,
   value_first string DEFAULT 'value_first_default',
   value_second string DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_simple_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(HASHED());

SELECT 'Dictionary hashed_dictionary_simple_key_simple_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.hashed_dictionary_simple_key_simple_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('01765_db.hashed_dictionary_simple_key_simple_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01765_db.hashed_dictionary_simple_key_simple_attributes ORDER BY id;

DROP DICTIONARY 01765_db.hashed_dictionary_simple_key_simple_attributes;

CREATE DICTIONARY 01765_db.sparse_hashed_dictionary_simple_key_simple_attributes
(
   id uint64,
   value_first string DEFAULT 'value_first_default',
   value_second string DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_simple_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(SPARSE_HASHED());

SELECT 'Dictionary sparse_hashed_dictionary_simple_key_simple_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('01765_db.sparse_hashed_dictionary_simple_key_simple_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01765_db.sparse_hashed_dictionary_simple_key_simple_attributes ORDER BY id;

DROP DICTIONARY 01765_db.sparse_hashed_dictionary_simple_key_simple_attributes;

DROP STREAM 01765_db.simple_key_simple_attributes_source_table;

create stream 01765_db.simple_key_complex_attributes_source_table
(
   id uint64,
   value_first string,
   value_second nullable(string)
)
;

INSERT INTO 01765_db.simple_key_complex_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO 01765_db.simple_key_complex_attributes_source_table VALUES(1, 'value_1', NULL);
INSERT INTO 01765_db.simple_key_complex_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

CREATE DICTIONARY 01765_db.hashed_dictionary_simple_key_complex_attributes
(
   id uint64,
   value_first string DEFAULT 'value_first_default',
   value_second nullable(string) DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_complex_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(HASHED());

SELECT 'Dictionary hashed_dictionary_simple_key_complex_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.hashed_dictionary_simple_key_complex_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('01765_db.hashed_dictionary_simple_key_complex_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01765_db.hashed_dictionary_simple_key_complex_attributes ORDER BY id;

DROP DICTIONARY 01765_db.hashed_dictionary_simple_key_complex_attributes;

CREATE DICTIONARY 01765_db.sparse_hashed_dictionary_simple_key_complex_attributes
(
   id uint64,
   value_first string DEFAULT 'value_first_default',
   value_second nullable(string) DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_complex_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(HASHED());

SELECT 'Dictionary sparse_hashed_dictionary_simple_key_complex_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dictGet('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_first', number, to_string('default')) as value_first,
    dictGetOrDefault('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('01765_db.sparse_hashed_dictionary_simple_key_complex_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01765_db.sparse_hashed_dictionary_simple_key_complex_attributes ORDER BY id;

DROP DICTIONARY 01765_db.sparse_hashed_dictionary_simple_key_complex_attributes;

DROP STREAM 01765_db.simple_key_complex_attributes_source_table;

create stream 01765_db.simple_key_hierarchy_table
(
    id uint64,
    parent_id uint64
) ();

INSERT INTO 01765_db.simple_key_hierarchy_table VALUES (1, 0);
INSERT INTO 01765_db.simple_key_hierarchy_table VALUES (2, 1);
INSERT INTO 01765_db.simple_key_hierarchy_table VALUES (3, 1);
INSERT INTO 01765_db.simple_key_hierarchy_table VALUES (4, 2);

CREATE DICTIONARY 01765_db.hashed_dictionary_simple_key_hierarchy
(
   id uint64,
   parent_id uint64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_hierarchy_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(HASHED());

SELECT 'Dictionary hashed_dictionary_simple_key_hierarchy';
SELECT 'dictGet';
SELECT dictGet('01765_db.hashed_dictionary_simple_key_hierarchy', 'parent_id', number) FROM system.numbers LIMIT 5;
SELECT 'dictGetHierarchy';
SELECT dictGetHierarchy('01765_db.hashed_dictionary_simple_key_hierarchy', to_uint64(1));
SELECT dictGetHierarchy('01765_db.hashed_dictionary_simple_key_hierarchy', to_uint64(4));

DROP DICTIONARY 01765_db.hashed_dictionary_simple_key_hierarchy;

CREATE DICTIONARY 01765_db.sparse_hashed_dictionary_simple_key_hierarchy
(
   id uint64,
   parent_id uint64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'simple_key_hierarchy_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(HASHED());

SELECT 'Dictionary sparse_hashed_dictionary_simple_key_hierarchy';
SELECT 'dictGet';
SELECT dictGet('01765_db.sparse_hashed_dictionary_simple_key_hierarchy', 'parent_id', number) FROM system.numbers LIMIT 5;
SELECT 'dictGetHierarchy';
SELECT dictGetHierarchy('01765_db.sparse_hashed_dictionary_simple_key_hierarchy', to_uint64(1));
SELECT dictGetHierarchy('01765_db.sparse_hashed_dictionary_simple_key_hierarchy', to_uint64(4));

DROP DICTIONARY 01765_db.sparse_hashed_dictionary_simple_key_hierarchy;

DROP STREAM 01765_db.simple_key_hierarchy_table;

DROP DATABASE 01765_db;
