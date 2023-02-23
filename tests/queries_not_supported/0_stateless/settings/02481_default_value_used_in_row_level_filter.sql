DROP STREAM IF EXISTS test_rlp;

CREATE STREAM test_rlp (a int32, b int32) ENGINE=MergeTree() ORDER BY a SETTINGS index_granularity=5;

INSERT INTO test_rlp SELECT number, number FROM numbers(15);

ALTER STREAM test_rlp ADD COLUMN c int32 DEFAULT b+10;

-- { echoOn }

SELECT a, c FROM test_rlp WHERE c%2 == 0 AND b < 5;

DROP POLICY IF EXISTS test_rlp_policy ON test_rlp;

CREATE ROW POLICY test_rlp_policy ON test_rlp FOR SELECT USING c%2 == 0 TO default;

SELECT a, c FROM test_rlp WHERE b < 5 SETTINGS optimize_move_to_prewhere = 0;

SELECT a, c FROM test_rlp PREWHERE b < 5;

-- { echoOff }

DROP POLICY test_rlp_policy ON test_rlp;

DROP STREAM test_rlp;
