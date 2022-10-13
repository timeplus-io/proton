SET check_query_single_value_result = 1;

DROP STREAM IF EXISTS test.hits_log;
DROP STREAM IF EXISTS test.hits_tinylog;
DROP STREAM IF EXISTS test.hits_stripelog;

CREATE TABLE test.hits_log (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID uint64)  ;
CREATE TABLE test.hits_tinylog (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID uint64) ;
CREATE TABLE test.hits_stripelog (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID uint64) ENGINE = StripeLog;

CHECK TABLE test.hits_log;
CHECK TABLE test.hits_tinylog;
CHECK TABLE test.hits_stripelog;

INSERT INTO test.hits_log SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID from table(test.hits);
INSERT INTO test.hits_tinylog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID from table(test.hits);
INSERT INTO test.hits_stripelog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID from table(test.hits);

SELECT count(), sum(cityHash64(CounterID, AdvEngineID, RegionID, SearchPhrase, UserID)) from table(test.hits);
SELECT count(), sum(cityHash64(*)) FROM test.hits_log;
SELECT count(), sum(cityHash64(*)) FROM test.hits_tinylog;
SELECT count(), sum(cityHash64(*)) FROM test.hits_stripelog;

CHECK TABLE test.hits_log;
CHECK TABLE test.hits_tinylog;
CHECK TABLE test.hits_stripelog;

DROP STREAM test.hits_log;
DROP STREAM test.hits_tinylog;
DROP STREAM test.hits_stripelog;
