-- Tags: no-parallel, no-fasttest

SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

SELECT '***date dict***';

create stream database_for_dict.date_table
(
  CountryID uint64,
  StartDate date,
  EndDate date,
  Tax float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO database_for_dict.date_table VALUES(1, to_date('2019-05-05'), to_date('2019-05-20'), 0.33);
INSERT INTO database_for_dict.date_table VALUES(1, to_date('2019-05-21'), to_date('2019-05-30'), 0.42);
INSERT INTO database_for_dict.date_table VALUES(2, to_date('2019-05-21'), to_date('2019-05-30'), 0.46);

CREATE DICTIONARY database_for_dict.dict1
(
  CountryID uint64,
  StartDate date,
  EndDate date,
  Tax float64
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', to_uint64(1), to_date('2019-05-15'));
SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', to_uint64(1), to_date('2019-05-29'));
SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', to_uint64(2), to_date('2019-05-29'));
SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', to_uint64(2), to_date('2019-05-31'));

SELECT '***datetime dict***';

create stream database_for_dict.datetime_table
(
  CountryID uint64,
  StartDate DateTime,
  EndDate DateTime,
  Tax float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO database_for_dict.datetime_table VALUES(1, to_datetime('2019-05-05 00:00:00'), to_datetime('2019-05-20 00:00:00'), 0.33);
INSERT INTO database_for_dict.datetime_table VALUES(1, to_datetime('2019-05-21 00:00:00'), to_datetime('2019-05-30 00:00:00'), 0.42);
INSERT INTO database_for_dict.datetime_table VALUES(2, to_datetime('2019-05-21 00:00:00'), to_datetime('2019-05-30 00:00:00'), 0.46);

CREATE DICTIONARY database_for_dict.dict2
(
  CountryID uint64,
  StartDate DateTime,
  EndDate DateTime,
  Tax float64
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'datetime_table' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', to_uint64(1), to_datetime('2019-05-15 00:00:00'));
SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', to_uint64(1), to_datetime('2019-05-29 00:00:00'));
SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', to_uint64(2), to_datetime('2019-05-29 00:00:00'));
SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', to_uint64(2), to_datetime('2019-05-31 00:00:00'));

SELECT '***hierarchy dict***';

create stream database_for_dict.table_with_hierarchy
(
  RegionID uint64,
  ParentRegionID uint64,
  RegionName string
)
ENGINE = MergeTree()
ORDER BY RegionID;

INSERT INTO database_for_dict.table_with_hierarchy VALUES (3, 2, 'Hamovniki'), (2, 1, 'Moscow'), (1, 10000, 'Russia') (7, 10000, 'Ulan-Ude');


CREATE DICTIONARY database_for_dict.dictionary_with_hierarchy
(
    RegionID uint64,
    ParentRegionID uint64 HIERARCHICAL,
    RegionName string
)
PRIMARY KEY RegionID
SOURCE(CLICKHOUSE(host 'localhost' port tcpPort() user 'default' db 'database_for_dict' table 'table_with_hierarchy'))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1000);

SELECT dictGetString('database_for_dict.dictionary_with_hierarchy', 'RegionName', to_uint64(2));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', to_uint64(3));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(3), to_uint64(2));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(7), to_uint64(10000));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', to_uint64(1), to_uint64(5));

DROP DATABASE IF EXISTS database_for_dict;
