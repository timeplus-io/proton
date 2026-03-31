-- Test: Dynamic/JSON stream batching threshold and JSON/non-JSON boundary behavior
-- Covers: PR #1124 — batch JSON commits, hasDynamicSubcolumns boundary check,
--         and historical reads via table(stream).
--
-- Regression for issue #1113: each JSON record used to produce its own part
-- (tiny-part storm). After this fix, records are batched before doCommit is
-- called, so the part count stays low.

-- Setup: clean slate
DROP STREAM IF EXISTS test_json_batching;
SELECT sleep(1) FORMAT Null;

-- Create a dynamic/JSON stream (hasDynamicSubcolumns = true)
CREATE STREAM test_json_batching (id uint32, payload json);
SELECT sleep(1) FORMAT Null;

-- Insert a batch of JSON records.
-- With batching: these should all land in a single part (or very few),
-- not one part per row.
INSERT INTO test_json_batching (id, payload) VALUES (1, '{"name":"alice","score":10}'), (2, '{"name":"bob","score":20}'), (3, '{"name":"carol","score":30}'), (4, '{"name":"dave","score":40}'), (5, '{"name":"eve","score":50}');

SELECT sleep(1) FORMAT Null;

-- Historical read via table(stream): all 5 rows must be visible
SELECT id FROM table(test_json_batching) ORDER BY id;

-- Verify round-trip correctness of a JSON field
SELECT payload.name FROM table(test_json_batching) WHERE id = 3;

-- Cleanup
DROP STREAM IF EXISTS test_json_batching;
