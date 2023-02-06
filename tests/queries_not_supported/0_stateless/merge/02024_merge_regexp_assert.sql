-- Tags: no-parallel
-- (databases can be removed in background, so this test should not be run in parallel)

DROP STREAM IF EXISTS t;
CREATE STREAM t (b uint8) ENGINE = Memory;
SELECT a FROM merge(REGEXP('.'), '^t$'); -- { serverError 47 }
SELECT a FROM merge(REGEXP('\0'), '^t$'); -- { serverError 47 }
SELECT a FROM merge(REGEXP('\0a'), '^t$'); -- { serverError 47 }
SELECT a FROM merge(REGEXP('\0a'), '^$'); -- { serverError 36 }
DROP STREAM t;
