DROP STREAM IF EXISTS merge_tree;
create stream merge_tree (x uint8) ENGINE = MergeTree ORDER BY x;
INSERT INTO merge_tree SELECT 0 FROM numbers(1000000);

SET max_threads = 4;
SET max_rows_to_read = 1100000;

SELECT count() FROM merge_tree;
SELECT count() FROM merge_tree;

SET max_rows_to_read = 900000;

-- constant ignore will be pruned by part pruner. ignore(*) is used.
SELECT count() FROM merge_tree WHERE not ignore(*); -- { serverError 158 }
SELECT count() FROM merge_tree WHERE not ignore(*); -- { serverError 158 }

DROP STREAM merge_tree;
