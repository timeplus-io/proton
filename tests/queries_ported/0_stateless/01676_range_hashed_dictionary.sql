DROP DATABASE IF EXISTS test_01676;

CREATE DATABASE test_01676;

CREATE STREAM test_01676.date_table
(
  CountryID uint64,
  Startdate date,
  Enddate date,
  Tax float64
)
ORDER BY CountryID;

INSERT INTO test_01676.date_table(CountryID, Startdate, Enddate, Tax) VALUES(1, to_date('2019-05-05'), to_date('2019-05-20'), 0.33);
INSERT INTO test_01676.date_table(CountryID, Startdate, Enddate, Tax) VALUES(1, to_date('2019-05-21'), to_date('2019-05-30'), 0.42);
INSERT INTO test_01676.date_table(CountryID, Startdate, Enddate, Tax) VALUES(2, to_date('2019-05-21'), to_date('2019-05-30'), 0.46);
SELECT sleep(3) FORMAT Null;

CREATE DICTIONARY range_dictionary
(
  CountryID uint64,
  Startdate date,
  Enddate date,
  Tax float64 DEFAULT 0.2
)
PRIMARY KEY CountryID
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'date_table' DB 'test_01676'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN Startdate MAX Enddate)
SETTINGS(dictionary_use_async_executor=1, max_threads=8)
;

SELECT 'Dictionary not nullable';
SELECT 'dict_get';
SELECT dict_get('range_dictionary', 'Tax', to_uint64(1), to_date('2019-05-15'));
SELECT dict_get('range_dictionary', 'Tax', to_uint64(1), to_date('2019-05-29'));
SELECT dict_get('range_dictionary', 'Tax', to_uint64(2), to_date('2019-05-29'));
SELECT dict_get('range_dictionary', 'Tax', to_uint64(2), to_date('2019-05-31'));
SELECT dict_get_or_default('range_dictionary', 'Tax', to_uint64(2), to_date('2019-05-31'), 0.4);
SELECT 'dict_has';
SELECT dict_has('range_dictionary', to_uint64(1), to_date('2019-05-15'));
SELECT dict_has('range_dictionary', to_uint64(1), to_date('2019-05-29'));
SELECT dict_has('range_dictionary', to_uint64(2), to_date('2019-05-29'));
SELECT dict_has('range_dictionary', to_uint64(2), to_date('2019-05-31'));
SELECT 'select columns from dictionary';
SELECT 'allColumns';
SELECT * FROM range_dictionary ORDER BY CountryID, Startdate, Enddate;
SELECT 'noColumns';
SELECT 1 FROM range_dictionary ORDER BY CountryID, Startdate, Enddate;
SELECT 'onlySpecificColumns';
SELECT CountryID, Startdate, Tax FROM range_dictionary ORDER BY CountryID, Startdate, Enddate;
SELECT 'onlySpecificColumn';
SELECT Tax FROM range_dictionary ORDER BY CountryID, Startdate, Enddate;

DROP DICTIONARY range_dictionary;
DROP STREAM test_01676.date_table;

CREATE STREAM test_01676.date_table
(
  CountryID uint64,
  Startdate date,
  Enddate date,
  Tax nullable(float64)
)
ORDER BY CountryID;

INSERT INTO test_01676.date_table(CountryID, Startdate, Enddate, Tax) VALUES(1, to_date('2019-05-05'), to_date('2019-05-20'), 0.33);
INSERT INTO test_01676.date_table(CountryID, Startdate, Enddate, Tax) VALUES(1, to_date('2019-05-21'), to_date('2019-05-30'), 0.42);
INSERT INTO test_01676.date_table(CountryID, Startdate, Enddate, Tax) VALUES(2, to_date('2019-05-21'), to_date('2019-05-30'), NULL);
SELECT sleep(3) FORMAT Null;

CREATE DICTIONARY range_dictionary_nullable
(
  CountryID uint64,
  Startdate date,
  Enddate date,
  Tax nullable(float64) DEFAULT 0.2
)
PRIMARY KEY CountryID
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'date_table' DB 'test_01676'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN Startdate MAX Enddate);

SELECT 'Dictionary nullable';
SELECT 'dict_get';
SELECT dict_get('range_dictionary_nullable', 'Tax', to_uint64(1), to_date('2019-05-15'));
SELECT dict_get('range_dictionary_nullable', 'Tax', to_uint64(1), to_date('2019-05-29'));
SELECT dict_get('range_dictionary_nullable', 'Tax', to_uint64(2), to_date('2019-05-29'));
SELECT dict_get('range_dictionary_nullable', 'Tax', to_uint64(2), to_date('2019-05-31'));
SELECT dict_get_or_default('range_dictionary_nullable', 'Tax', to_uint64(2), to_date('2019-05-31'), 0.4);
SELECT 'dict_has';
SELECT dict_has('range_dictionary_nullable', to_uint64(1), to_date('2019-05-15'));
SELECT dict_has('range_dictionary_nullable', to_uint64(1), to_date('2019-05-29'));
SELECT dict_has('range_dictionary_nullable', to_uint64(2), to_date('2019-05-29'));
SELECT dict_has('range_dictionary_nullable', to_uint64(2), to_date('2019-05-31'));
SELECT 'select columns from dictionary';
SELECT 'allColumns';
SELECT * FROM range_dictionary_nullable ORDER BY CountryID, Startdate, Enddate;
SELECT 'noColumns';
SELECT 1 FROM range_dictionary_nullable ORDER BY CountryID, Startdate, Enddate;
SELECT 'onlySpecificColumns';
SELECT CountryID, Startdate, Tax FROM range_dictionary_nullable ORDER BY CountryID, Startdate, Enddate;
SELECT 'onlySpecificColumn';
SELECT Tax FROM range_dictionary_nullable ORDER BY CountryID, Startdate, Enddate;

DROP DICTIONARY range_dictionary_nullable;
DROP STREAM test_01676.date_table;

