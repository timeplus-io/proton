-- https://github.com/ClickHouse/ClickHouse/issues/52019
DROP STREAM IF EXISTS set_index__fuzz_41;
CREATE STREAM set_index__fuzz_41 (`a` Date, `b` nullable(DateTime64(3)), INDEX b_set b TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple_cast();
INSERT INTO set_index__fuzz_41 (a) VALUES (today());
SELECT b FROM set_index__fuzz_41 WHERE and(b = 256) SETTINGS force_data_skipping_indices = 'b_set', optimize_move_to_prewhere = 0, max_parallel_replicas=2; -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
DROP STREAM set_index__fuzz_41;
