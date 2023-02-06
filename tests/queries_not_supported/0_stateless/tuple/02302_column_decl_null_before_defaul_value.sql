select 'create stream, column +type +NULL';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column +type +NOT NULL';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column +type +NULL +DEFAULT';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column +type +NOT NULL +DEFAULT';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT NOT NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column +type +DEFAULT +NULL';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT DEFAULT 1 NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column +type +DEFAULT +NOT NULL';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT DEFAULT 1 NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column -type +NULL +DEFAULT';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column -type +NOT NULL +DEFAULT';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id NOT NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column -type +DEFAULT +NULL';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id DEFAULT 1 NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'create stream, column -type +DEFAULT +NOT NULL';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id DEFAULT 1 NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM null_before;

select 'alter column, NULL modifier is not allowed';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
ALTER STREAM null_before ALTER COLUMN id TYPE INT NULL; -- { clientError SYNTAX_ERROR }

select 'modify column, NULL modifier is not allowed';
DROP STREAM IF EXISTS null_before SYNC;
CREATE STREAM null_before (id INT NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
ALTER STREAM null_before MODIFY COLUMN id NULL DEFAULT 1; -- { serverError UNKNOWN_TYPE }

DROP STREAM IF EXISTS null_before SYNC;
