-- Tags: no-parallel

DROP STREAM IF EXISTS date_table;
create stream date_table
(
  CountryID uint64,
  CountryKey string,
  StartDate date,
  EndDate date,
  Tax float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO date_table VALUES(1, '1', to_date('2019-05-05'), to_date('2019-05-20'), 0.33);
INSERT INTO date_table VALUES(1, '1', to_date('2019-05-21'), to_date('2019-05-30'), 0.42);
INSERT INTO date_table VALUES(2, '2', to_date('2019-05-21'), to_date('2019-05-30'), 0.46);

DROP DICTIONARY IF EXISTS range_dictionary;
CREATE DICTIONARY range_dictionary
(
  CountryID uint64,
  CountryKey string,
  StartDate date,
  EndDate date,
  Tax float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT 'Dictionary not nullable';
SELECT 'dictGet';
SELECT dictGet('range_dictionary', 'Tax', (to_uint64(1), '1'), to_date('2019-05-15'));
SELECT dictGet('range_dictionary', 'Tax', (to_uint64(1), '1'), to_date('2019-05-29'));
SELECT dictGet('range_dictionary', 'Tax', (to_uint64(2), '2'), to_date('2019-05-29'));
SELECT dictGet('range_dictionary', 'Tax', (to_uint64(2), '2'), to_date('2019-05-31'));
SELECT dictGetOrDefault('range_dictionary', 'Tax', (to_uint64(2), '2'), to_date('2019-05-31'), 0.4);
SELECT 'dictHas';
SELECT dictHas('range_dictionary', (to_uint64(1), '1'), to_date('2019-05-15'));
SELECT dictHas('range_dictionary', (to_uint64(1), '1'), to_date('2019-05-29'));
SELECT dictHas('range_dictionary', (to_uint64(2), '2'), to_date('2019-05-29'));
SELECT dictHas('range_dictionary', (to_uint64(2), '2'), to_date('2019-05-31'));
SELECT 'select columns from dictionary';
SELECT 'allColumns';
SELECT * FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;
SELECT 'noColumns';
SELECT 1 FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumns';
SELECT CountryID, StartDate, Tax FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumn';
SELECT Tax FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;

DROP STREAM date_table;
DROP DICTIONARY range_dictionary;

create stream date_table
(
  CountryID uint64,
  CountryKey string,
  StartDate date,
  EndDate date,
  Tax nullable(float64)
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO date_table VALUES(1, '1', to_date('2019-05-05'), to_date('2019-05-20'), 0.33);
INSERT INTO date_table VALUES(1, '1', to_date('2019-05-21'), to_date('2019-05-30'), 0.42);
INSERT INTO date_table VALUES(2, '2', to_date('2019-05-21'), to_date('2019-05-30'), NULL);

CREATE DICTIONARY range_dictionary_nullable
(
  CountryID uint64,
  CountryKey string,
  StartDate date,
  EndDate date,
  Tax nullable(float64) DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT 'Dictionary nullable';
SELECT 'dictGet';
SELECT dictGet('range_dictionary_nullable', 'Tax', (to_uint64(1), '1'), to_date('2019-05-15'));
SELECT dictGet('range_dictionary_nullable', 'Tax', (to_uint64(1), '1'), to_date('2019-05-29'));
SELECT dictGet('range_dictionary_nullable', 'Tax', (to_uint64(2), '2'), to_date('2019-05-29'));
SELECT dictGet('range_dictionary_nullable', 'Tax', (to_uint64(2), '2'), to_date('2019-05-31'));
SELECT dictGetOrDefault('range_dictionary_nullable', 'Tax', (to_uint64(2), '2'), to_date('2019-05-31'), 0.4);
SELECT 'dictHas';
SELECT dictHas('range_dictionary_nullable', (to_uint64(1), '1'), to_date('2019-05-15'));
SELECT dictHas('range_dictionary_nullable', (to_uint64(1), '1'), to_date('2019-05-29'));
SELECT dictHas('range_dictionary_nullable', (to_uint64(2), '2'), to_date('2019-05-29'));
SELECT dictHas('range_dictionary_nullable', (to_uint64(2), '2'), to_date('2019-05-31'));
SELECT 'select columns from dictionary';
SELECT 'allColumns';
SELECT * FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;
SELECT 'noColumns';
SELECT 1 FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumns';
SELECT CountryID, StartDate, Tax FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumn';
SELECT Tax FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;

DROP STREAM date_table;
DROP DICTIONARY range_dictionary_nullable;
