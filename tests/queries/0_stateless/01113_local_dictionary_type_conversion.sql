-- Tags: no-parallel

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

create stream database_for_dict.table_for_dict (
  CompanyID string,
  OSType Enum('UNKNOWN' = 0, 'WINDOWS' = 1, 'LINUX' = 2, 'ANDROID' = 3, 'MAC' = 4),
  SomeID int32
)
();

INSERT INTO database_for_dict.table_for_dict VALUES ('First', 'WINDOWS', 1), ('Second', 'LINUX', 2);

CREATE DICTIONARY database_for_dict.dict_with_conversion
(
  CompanyID string DEFAULT '',
  OSType string DEFAULT '',
  SomeID int32 DEFAULT 0
)
PRIMARY KEY CompanyID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 20)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT * FROM database_for_dict.dict_with_conversion ORDER BY CompanyID;

DROP DATABASE IF EXISTS database_for_dict;
