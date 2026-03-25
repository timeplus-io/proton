-- Regression test for https://github.com/timeplus-io/proton-enterprise/issues/11731
-- "Context has expired" error when json_value() is used inside an IN subquery
-- against table(stream) or table(materialized_view).
--
-- Root cause: FunctionSQLJSON inherited WithConstContext (storing a weak_ptr to
-- the query context). When the IN subquery plan was merged into the main plan
-- via addPlansForSets, the subquery's interpreter context was dropped, causing
-- getContext() to throw "Context has expired" during pipeline construction.
--
-- Fix: Remove WithConstContext from FunctionSQLJSON and capture the required
-- settings (max_parser_depth, allow_simdjson, etc.) as plain member variables
-- at construction time (equivalent to ClickHouse PR #68534).

DROP VIEW IF EXISTS 99914_mv;
DROP STREAM IF EXISTS 99914_stream;

CREATE STREAM 99914_stream (id string);
CREATE MATERIALIZED VIEW 99914_mv AS SELECT * FROM 99914_stream;

-- The query below previously crashed with "Context has expired" due to
-- FunctionSQLJSON holding a weak_ptr to the subquery interpreter context.
-- It should now return an empty result set without error.
SELECT * FROM table(99914_stream)
WHERE id IN (
    SELECT json_value(id, '$.id')
    FROM table(99914_mv)
);

-- Also verify json_value itself works correctly in a standalone context.
SELECT json_value('{"id":"hello"}', '$.id');

DROP VIEW 99914_mv;
DROP STREAM 99914_stream;
