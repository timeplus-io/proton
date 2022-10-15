SET check_query_single_value_result = 1;

DROP STREAM IF EXISTS test.hits_log;
DROP STREAM IF EXISTS test.hits_tinylog;
DROP STREAM IF EXISTS test.hits_stripelog;

CREATE STREAM test.hits_log (CounterID uint32, AdvEngineID uint8, RegionID uint32, SearchPhrase string, UserID uint64)  ;
CREATE STREAM test.hits_tinylog (CounterID uint32, AdvEngineID uint8, RegionID uint32, SearchPhrase string, UserID uint64) ;
CREATE STREAM test.hits_stripelog (CounterID uint32, AdvEngineID uint8, RegionID uint32, SearchPhrase string, UserID uint64) ENGINE = StripeLog;

CHECK TABLE test.hits_log;
CHECK TABLE test.hits_tinylog;
CHECK TABLE test.hits_stripelog;

INSERT INTO test.hits_log SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID from table(test.hits);
INSERT INTO test.hits_tinylog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID from table(test.hits);
INSERT INTO test.hits_stripelog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID from table(test.hits);
select sleep(3);

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
