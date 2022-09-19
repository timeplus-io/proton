-- Tags: no-parallel

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

use database_for_dict;

create stream date_table
(
  id uint32,
  val string,
  start date,
  end date
) Engine = Memory();

INSERT INTO date_table VALUES(1, '1', to_date('2019-01-05'), to_date('2020-01-10'));

CREATE DICTIONARY somedict
(
  id uint32,
  val string,
  start date,
  end date
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB 'database_for_dict'))
LAYOUT(RANGE_HASHED())
RANGE (MIN start MAX end)
LIFETIME(MIN 300 MAX 360);

SELECT * from somedict;

-- No dictionary columns
SELECT 1 FROM somedict;

SHOW TABLES;

DROP DATABASE database_for_dict;
