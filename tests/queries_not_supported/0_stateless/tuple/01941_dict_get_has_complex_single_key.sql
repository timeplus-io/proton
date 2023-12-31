DROP STREAM IF EXISTS test_dictionary_source;
create stream test_dictionary_source (key string, value string) ;

INSERT INTO test_dictionary_source VALUES ('Key', 'Value');

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary(key string, value string)
PRIMARY KEY key
LAYOUT(COMPLEX_KEY_HASHED())
SOURCE(CLICKHOUSE(TABLE 'test_dictionary_source'))
LIFETIME(0);

SELECT 'dictGet';
SELECT dictGet('test_dictionary', 'value', tuple('Key'));
SELECT dictGet('test_dictionary', 'value', tuple(materialize('Key')));
SELECT dictGet('test_dictionary', 'value', 'Key');
SELECT dictGet('test_dictionary', 'value', materialize('Key'));

SELECT 'dictHas';
SELECT dictHas('test_dictionary', tuple('Key'));
SELECT dictHas('test_dictionary', tuple(materialize('Key')));
SELECT dictHas('test_dictionary', 'Key');
SELECT dictHas('test_dictionary', materialize('Key'));

DROP DICTIONARY test_dictionary;
DROP STREAM test_dictionary_source;
