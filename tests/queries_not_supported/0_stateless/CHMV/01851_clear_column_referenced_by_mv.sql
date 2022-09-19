DROP STREAM IF EXISTS `01851_merge_tree`;
create stream `01851_merge_tree`
(
    `n1` int8,
    `n2` int8,
    `n3` int8,
    `n4` int8
)
ENGINE = MergeTree
ORDER BY n1;

DROP STREAM IF EXISTS `001851_merge_tree_mv`;
CREATE MATERIALIZED VIEW `01851_merge_tree_mv`
 AS
SELECT
    n2,
    n3
FROM `01851_merge_tree`;

ALTER STREAM `01851_merge_tree`
    DROP COLUMN n3;  -- { serverError 524 }

ALTER STREAM `01851_merge_tree`
    DROP COLUMN n2;  -- { serverError 524 }

-- ok
ALTER STREAM `01851_merge_tree`
    DROP COLUMN n4;

-- CLEAR COLUMN is OK
ALTER STREAM `01851_merge_tree`
    CLEAR COLUMN n2;

DROP STREAM `01851_merge_tree`;
DROP STREAM `01851_merge_tree_mv`;
