SET optimize_on_insert = 0;

DROP STREAM IF EXISTS old_style;
CREATE STREAM old_style(d Date, x uint32) ENGINE MergeTree(d, x, 8192);
ALTER STREAM old_style ADD COLUMN y uint32, MODIFY ORDER BY (x, y); -- { serverError 36}
DROP STREAM old_style;

DROP STREAM IF EXISTS summing;
CREATE STREAM summing(x uint32, y uint32, val uint32) ENGINE SummingMergeTree ORDER BY (x, y);

/* Can't add an expression with existing column to ORDER BY. */
ALTER STREAM summing MODIFY ORDER BY (x, y, -val); -- { serverError 36}

/* Can't add an expression with existing column to ORDER BY. */
ALTER STREAM summing ADD COLUMN z uint32 DEFAULT x + 1, MODIFY ORDER BY (x, y, -z); -- { serverError 36}

/* Can't add nonexistent column to ORDER BY. */
ALTER STREAM summing MODIFY ORDER BY (x, y, nonexistent); -- { serverError 47}

/* Can't modyfy ORDER BY so that it is no longer a prefix of the PRIMARY KEY. */
ALTER STREAM summing MODIFY ORDER BY x; -- { serverError 36}

ALTER STREAM summing ADD COLUMN z uint32 AFTER y, MODIFY ORDER BY (x, y, -z);

INSERT INTO summing(x, y, z, val) values (1, 2, 0, 10), (1, 2, 1, 30), (1, 2, 2, 40);

SELECT '*** Check that the parts are sorted according to the new key. ***';
SELECT * FROM summing;

INSERT INTO summing(x, y, z, val) values (1, 2, 0, 20), (1, 2, 2, 50);

SELECT '*** Check that the rows are collapsed according to the new key. ***';
SELECT * FROM summing FINAL ORDER BY x, y, z;

SELECT '*** Check SHOW CREATE STREAM ***';
SHOW CREATE STREAM summing;

DROP STREAM summing;
