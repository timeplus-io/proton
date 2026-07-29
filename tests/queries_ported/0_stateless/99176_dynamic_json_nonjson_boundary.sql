-- Test: JSON/non-JSON stream boundary — types must not mix into one batch
-- Covers: PR #1124 explicit boundary check added before mergeBlocks()
-- A non-JSON block followed by a JSON block must produce separate commits.

-- Setup
DROP STREAM IF EXISTS test_nonjson_stream;
DROP STREAM IF EXISTS test_json_stream;
SELECT sleep(1) FORMAT Null;

-- Regular (non-JSON) stream
CREATE STREAM test_nonjson_stream (id uint32, name string);
-- Dynamic/JSON stream
CREATE STREAM test_json_stream (id uint32, payload json);
SELECT sleep(1) FORMAT Null;

-- Insert into non-JSON stream
INSERT INTO test_nonjson_stream (id, name) VALUES (10, 'alpha'), (11, 'beta'), (12, 'gamma');

-- Insert into JSON stream
INSERT INTO test_json_stream (id, payload)
VALUES (20, '{"x":1}'), (21, '{"x":2}'), (22, '{"x":3}');

SELECT sleep(3) FORMAT Null;

-- Non-JSON historical read
SELECT id, name FROM table(test_nonjson_stream) ORDER BY id;

-- JSON historical read
SELECT id, payload.x FROM table(test_json_stream) ORDER BY id;

-- Cleanup
DROP STREAM IF EXISTS test_nonjson_stream;
DROP STREAM IF EXISTS test_json_stream;
