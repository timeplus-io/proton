DROP STREAM IF EXISTS test_log;
DROP STREAM IF EXISTS test_tiny_log;

create stream test_log (x uint8, s string, a array(Nullable(string)))  ;
create stream test_tiny_log (x uint8, s string, a array(Nullable(string))) ;

INSERT INTO test_log VALUES (64, 'Value1', ['Value2', 'Value3', NULL]);
INSERT INTO test_tiny_log VALUES (64, 'Value1', ['Value2', 'Value3', NULL]);

SELECT data_compressed_bytes FROM system.columns WHERE table = 'test_log' AND database = currentDatabase();
SELECT data_compressed_bytes FROM system.columns WHERE table = 'test_tiny_log' AND database = currentDatabase();

DROP STREAM test_log;
DROP STREAM test_tiny_log;