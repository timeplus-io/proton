DROP STREAM IF EXISTS test.hits_dst;
DROP STREAM IF EXISTS test.hits_buffer;

CREATE STREAM test.hits_dst AS test.hits;
CREATE STREAM test.hits_buffer AS test.hits_dst ENGINE = Buffer(test, hits_dst, 8, 1, 10, 10000, 100000, 10000000, 100000000);

INSERT INTO test.hits_buffer SELECT * from table(test.hits) WHERE CounterID = 800784;
SELECT count() FROM test.hits_buffer;
SELECT count() FROM test.hits_dst;

OPTIMIZE STREAM test.hits_buffer;
SELECT count() FROM test.hits_buffer;
SELECT count() FROM test.hits_dst;

DROP STREAM test.hits_dst;
DROP STREAM test.hits_buffer;
