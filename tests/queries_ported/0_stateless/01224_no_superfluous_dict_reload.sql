-- Tags: no-parallel

DROP DATABASE IF EXISTS dict_db_01224 CASCADE;
DROP DATABASE IF EXISTS dict_db_01224_dictionary CASCADE;
-- set allow_deprecated_database_ordinary=1;
-- Create stream/dictionary in Oridinary database will have issue as not stored in MetaDB
-- CREATE DATABASE dict_db_01224 ENGINE=Ordinary;  -- Different internal dictionary name with Atomic
CREATE DATABASE dict_db_01224;
CREATE DATABASE dict_db_01224_dictionary Engine=Dictionary;

CREATE STREAM dict_db_01224.dict_data (key uint64, val uint64) Engine=Memory();
CREATE DICTIONARY dict_db_01224.dict
(
  key uint64 DEFAULT 0,
  val uint64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'dict_data' PASSWORD '' DB 'dict_db_01224'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SELECT * FROM system.tables FORMAT Null;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SHOW CREATE dict_db_01224.dict FORMAT TSVRaw;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

-- FIXME: load Dictionary database
-- SHOW CREATE dict_db_01224_dictionary.`dict_db_01224.dict` FORMAT TSVRaw;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SELECT engine, metadata_path LIKE '%/store/%/dict.sql', create_table_query FROM system.tables WHERE database = 'dict_db_01224' AND name = 'dict';
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

SELECT name, type FROM system.columns WHERE database = 'dict_db_01224' AND table = 'dict';
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

DROP DICTIONARY dict_db_01224.dict;
SELECT status FROM system.dictionaries WHERE database = 'dict_db_01224' AND name = 'dict';

DROP DATABASE dict_db_01224 CASCADE;
DROP DATABASE dict_db_01224_dictionary CASCADE;
