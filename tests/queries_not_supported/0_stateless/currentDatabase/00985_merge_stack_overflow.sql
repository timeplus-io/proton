-- Tags: no-parallel
--       ^^^^^^^^^^^ otherwise you may hit TOO_DEEP_RECURSION error during querying system.columns

DROP STREAM IF EXISTS merge1;
DROP STREAM IF EXISTS merge2;

create stream IF NOT EXISTS merge1 (x uint64) ENGINE = Merge(currentDatabase(), '^merge\\d$');
create stream IF NOT EXISTS merge2 (x uint64) ENGINE = Merge(currentDatabase(), '^merge\\d$');

SELECT * FROM merge1; -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM merge2; -- { serverError TOO_DEEP_RECURSION }

DROP STREAM merge1;
DROP STREAM merge2;
