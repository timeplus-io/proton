-- Regression test for https://github.com/timeplus-io/proton-enterprise/issues/11203
-- "Create complex VIEW is very slow"
--
-- Root cause: StorageView::isStreamingQuery() rebuilt a full
-- InterpreterSelectWithUnionQuery for every nested view at every recursion
-- level of JoinedTables::resolveTables, giving ~O(N^2) analyzer cost on
-- N-deep view chains. Layered Splunk-TA-style views took tens of seconds
-- to create.
--
-- Fix: query-scoped memoization via QueryAnalysisCache on the Context.
-- Keyed by (StorageID, metadata-pointer, query_mode)
-- so cross-mode callers never share a cached answer. Historical table(view)
-- calls take a fast-path and never touch the cache. Parameterized views
-- keep their original (uncached) behavior.
--
-- This is a correctness-only test. Each layered CREATE VIEW below runs the
-- cached streaming-query branch during its own getSampleBlock validation; if
-- the cache key or short-circuit were wrong, one of these CREATEs would fail
-- or the table() reads below would see corrupted output. Perf is verified
-- out-of-band with the reproducer in tmp/11203/.

DROP VIEW   IF EXISTS 99917_final;
DROP VIEW   IF EXISTS 99917_v4;
DROP VIEW   IF EXISTS 99917_v3;
DROP VIEW   IF EXISTS 99917_v2;
DROP VIEW   IF EXISTS 99917_v1;
DROP STREAM IF EXISTS 99917_src;

CREATE STREAM 99917_src (id int64, payload string);

-- 5-layer view chain. Each CREATE VIEW here validates its SELECT via
-- InterpreterSelectWithUnionQuery::getSampleBlock, which calls back into
-- StorageView::isStreamingQuery on every referenced child view. Before the
-- fix, this was the quadratic recursion path.
CREATE VIEW 99917_v1    AS SELECT id, payload, length(payload) AS plen FROM 99917_src;
CREATE VIEW 99917_v2    AS SELECT *, plen * 2 AS plen2 FROM 99917_v1;
CREATE VIEW 99917_v3    AS SELECT *, if(plen2 > 10, 'big', 'small') AS bucket FROM 99917_v2;
CREATE VIEW 99917_v4    AS SELECT *, upper(bucket) AS bucket_upper FROM 99917_v3;
CREATE VIEW 99917_final AS SELECT id, payload, plen, plen2, bucket, bucket_upper FROM 99917_v4;

-- Seed data for the historical read below.
INSERT INTO 99917_src (id, payload) VALUES (1, 'hello world');
SELECT sleep(2) FORMAT Null;

-- Historical mode: exercises the query_mode=="table" short-circuit inside
-- StorageView::isStreamingQuery. Must still produce the expected projection
-- for every layer of the chain.
SELECT id, payload, plen, plen2, bucket, bucket_upper
FROM table(99917_final)
ORDER BY id;

-- Repeat the read: the interpreter re-enters isStreamingQuery for each nested
-- view and must get the same answer as before — either via the short-circuit
-- or a cache hit. Result must be identical.
SELECT id, payload, plen, plen2, bucket, bucket_upper
FROM table(99917_final)
ORDER BY id;

DROP VIEW   99917_final;
DROP VIEW   99917_v4;
DROP VIEW   99917_v3;
DROP VIEW   99917_v2;
DROP VIEW   99917_v1;
DROP STREAM 99917_src;
