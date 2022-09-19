-- Tags: no-parallel

DROP DATABASE IF EXISTS dict_db_02179;
CREATE DATABASE dict_db_02179;

create stream dict_db_02179.dict_data (key uint64, val uint64) Engine=Memory();
CREATE DICTIONARY dict_db_02179.dict
(
  key uint64 DEFAULT 0,
  val uint64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB 'dict_db_02179'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

INSERT INTO dict_db_02179.dict_data VALUES(1,11);

SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_02179' AND name = 'dict';
SELECT dictGetUInt64('dict_db_02179.dict', 'val', to_uint64(0));
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_02179' AND name = 'dict';


SELECT 'SYSTEM RELOAD DICTIONARIES ON CLUSTER test_shard_localhost';
SET distributed_ddl_output_mode='throw';
SYSTEM RELOAD DICTIONARIES ON CLUSTER test_shard_localhost;
SET distributed_ddl_output_mode='none';
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_02179' AND name = 'dict';
SELECT dictGetUInt64('dict_db_02179.dict', 'val', to_uint64(1));
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_02179' AND name = 'dict';

SELECT 'CREATE DATABASE';
DROP DATABASE IF EXISTS empty_db_02179;
CREATE DATABASE empty_db_02179;
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_02179' AND name = 'dict';

DROP DICTIONARY dict_db_02179.dict;
DROP STREAM dict_db_02179.dict_data;
DROP DATABASE dict_db_02179;
DROP DATABASE empty_db_02179;
