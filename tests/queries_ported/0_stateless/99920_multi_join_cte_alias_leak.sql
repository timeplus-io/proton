-- https://github.com/timeplus-io/proton-enterprise/issues/11990
-- A CTE with >=2 inner JOINs whose SELECT list uses qualified identifiers
-- (e.g. a.userid) without explicit AS must not leak the inner alias into
-- the CTE's output column names; downstream qualified references like
-- t4.userid must resolve.
--
-- The bug is in parse-time alias rewriting (JoinToSubqueryTransformVisitor),
-- so wrapping the base streams in table(...) keeps the fix exercised in
-- historical mode and removes any streaming-runtime flakiness.

DROP STREAM IF EXISTS issue_11990_a;
DROP STREAM IF EXISTS issue_11990_b;
DROP STREAM IF EXISTS issue_11990_c;

CREATE STREAM issue_11990_a (userid string, securityid string, sx string, qty float64);
CREATE STREAM issue_11990_b (sxc_fee string, fee float64);
CREATE STREAM issue_11990_c (SecurityID string, LastPx float64);

INSERT INTO issue_11990_a(userid, securityid, sx, qty) VALUES ('u1', 's1', 'x', 100);
INSERT INTO issue_11990_b(sxc_fee, fee) VALUES ('x', 0.001);
INSERT INTO issue_11990_c(SecurityID, LastPx) VALUES ('s1', 1.0);

-- Wait for append-stream commits to be visible to historical reads.
SELECT sleep(3) FORMAT Null;

-- Downstream LEFT JOIN against a multi-JOIN CTE must find t4.userid/t4.securityid:
-- inside the CTE body the no-clash qualified identifier `a.userid` is exposed as
-- short column `userid`, so the outer `t4.userid` resolves. At top level (non-CTE)
-- qualified output names are preserved; see 00820_multiple_joins for that behavior.
WITH
  inner_cte AS (
    SELECT a.userid, a.securityid, a.qty * c.LastPx * b.fee AS uprofit
    FROM table(issue_11990_a) AS a
    JOIN table(issue_11990_c) AS c ON a.securityid = c.SecurityID
    JOIN table(issue_11990_b) AS b ON a.sx = b.sxc_fee
  )
SELECT s.userid, t4.uprofit
FROM table(issue_11990_a) AS s
LEFT JOIN inner_cte AS t4
  ON s.userid = t4.userid AND s.securityid = t4.securityid;

-- Same gating must hold for an inline derived-table (no WITH). The inner
-- multi-JOIN is a subquery body, so `a.userid` is exposed as `userid` and
-- outer `t.userid` resolves.
SELECT t.userid, t.uprofit
FROM (
    SELECT a.userid, a.qty * c.LastPx * b.fee AS uprofit
    FROM table(issue_11990_a) AS a
    JOIN table(issue_11990_c) AS c ON a.securityid = c.SecurityID
    JOIN table(issue_11990_b) AS b ON a.sx = b.sxc_fee
) AS t;

DROP STREAM issue_11990_a;
DROP STREAM issue_11990_b;
DROP STREAM issue_11990_c;

-- Clash-inside-CTE case: short name `userid` exists in both x and y so
-- `has_clash` is true. The fix's `has_clash || !is_subquery` keeps the
-- long name captured here so the inner SELECT can still disambiguate the
-- two columns. A future refactor that drops `has_clash` from the disjunct
-- would silently break disambiguation while the no-clash branch above
-- still passes — this query pins that branch.

DROP STREAM IF EXISTS issue_11990_x;
DROP STREAM IF EXISTS issue_11990_y;
DROP STREAM IF EXISTS issue_11990_z;

CREATE STREAM issue_11990_x (userid string, k string);
CREATE STREAM issue_11990_y (userid string, k string, v float64);
CREATE STREAM issue_11990_z (k string, w float64);

INSERT INTO issue_11990_x(userid, k) VALUES ('ux', 'k1');
INSERT INTO issue_11990_y(userid, k, v) VALUES ('uy', 'k1', 1.5);
INSERT INTO issue_11990_z(k, w) VALUES ('k1', 2.0);

SELECT sleep(3) FORMAT Null;

WITH inner_clash AS (
    SELECT x.userid, y.userid, y.v * z.w AS prod
    FROM table(issue_11990_x) AS x
    JOIN table(issue_11990_y) AS y ON x.k = y.k
    JOIN table(issue_11990_z) AS z ON y.k = z.k
)
SELECT * FROM inner_clash;

DROP STREAM issue_11990_x;
DROP STREAM issue_11990_y;
DROP STREAM issue_11990_z;
