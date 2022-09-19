-- MergeTree
DROP STREAM IF EXISTS `01746_merge_tree`;
create stream `01746_merge_tree`
(
    `n1` int8,
    `n2` int8,
    `n3` int8,
    `n4` int8
)
ENGINE = MergeTree
ORDER BY n1;

DROP STREAM IF EXISTS `01746_merge_tree_mv`;
CREATE MATERIALIZED VIEW `01746_merge_tree_mv`
 AS
SELECT
    n2,
    n3
FROM `01746_merge_tree`;

ALTER STREAM `01746_merge_tree`
    DROP COLUMN n3;  -- { serverError 524 }

ALTER STREAM `01746_merge_tree`
    DROP COLUMN n2;  -- { serverError 524 }

-- ok
ALTER STREAM `01746_merge_tree`
    DROP COLUMN n4;

DROP STREAM `01746_merge_tree`;
DROP STREAM `01746_merge_tree_mv`;

-- Null 
DROP STREAM IF EXISTS `01746_null`;
create stream `01746_null`
(
    `n1` int8,
    `n2` int8,
    `n3` int8
)
ENGINE = Null;

DROP STREAM IF EXISTS `01746_null_mv`;
CREATE MATERIALIZED VIEW `01746_null_mv`
 AS
SELECT
    n1,
    n2
FROM `01746_null`;

ALTER STREAM `01746_null`
    DROP COLUMN n1; -- { serverError 524 }

ALTER STREAM `01746_null`
    DROP COLUMN n2; -- { serverError 524 }

-- ok
ALTER STREAM `01746_null`
    DROP COLUMN n3;

DROP STREAM `01746_null`;
DROP STREAM `01746_null_mv`;

-- Distributed

DROP STREAM IF EXISTS `01746_local`;
create stream `01746_local`
(
    `n1` int8,
    `n2` int8,
    `n3` int8
)
;

DROP STREAM IF EXISTS `01746_dist`;
create stream `01746_dist` AS `01746_local`
ENGINE = Distributed('test_shard_localhost', currentDatabase(), `01746_local`, rand());

DROP STREAM IF EXISTS `01746_dist_mv`;
CREATE MATERIALIZED VIEW `01746_dist_mv`
 AS
SELECT
    n1,
    n2
FROM `01746_dist`;

ALTER STREAM `01746_dist`
    DROP COLUMN n1; -- { serverError 524 }

ALTER STREAM `01746_dist`
    DROP COLUMN n2; -- { serverError 524 }

-- ok
ALTER STREAM `01746_dist`
    DROP COLUMN n3;

DROP STREAM `01746_local`;
DROP STREAM `01746_dist`;
DROP STREAM `01746_dist_mv`;

-- Merge
DROP STREAM IF EXISTS `01746_merge_t`;
create stream `01746_merge_t`
(
    `n1` int8,
    `n2` int8,
    `n3` int8
)
;

DROP STREAM IF EXISTS `01746_merge`;
create stream `01746_merge` AS `01746_merge_t`
ENGINE = Merge(currentDatabase(), '01746_merge_t');

DROP STREAM IF EXISTS `01746_merge_mv`;
CREATE MATERIALIZED VIEW `01746_merge_mv`
 AS
SELECT
    n1,
    n2
FROM `01746_merge`;

ALTER STREAM `01746_merge`
    DROP COLUMN n1; -- { serverError 524 }

ALTER STREAM `01746_merge`
    DROP COLUMN n2; -- { serverError 524 }

-- ok
ALTER STREAM `01746_merge`
    DROP COLUMN n3;

DROP STREAM `01746_merge_t`;
DROP STREAM `01746_merge`;
DROP STREAM `01746_merge_mv`;

-- Buffer
DROP STREAM IF EXISTS `01746_buffer_t`;
create stream `01746_buffer_t`
(
    `n1` int8,
    `n2` int8,
    `n3` int8
)
;

DROP STREAM IF EXISTS `01746_buffer`;
create stream `01746_buffer` AS `01746_buffer_t`
ENGINE = Buffer(currentDatabase(), `01746_buffer_t`, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

DROP STREAM IF EXISTS `01746_buffer_mv`;
CREATE MATERIALIZED VIEW `01746_buffer_mv`
 AS
SELECT
    n1,
    n2
FROM `01746_buffer`;

ALTER STREAM `01746_buffer`
    DROP COLUMN n1; -- { serverError 524 }

ALTER STREAM `01746_buffer`
    DROP COLUMN n2; -- { serverError 524 }

-- ok
ALTER STREAM `01746_buffer`
    DROP COLUMN n3;

DROP STREAM `01746_buffer_t`;
DROP STREAM `01746_buffer`;
DROP STREAM `01746_buffer_mv`;
