DROP STREAM IF EXISTS test_apply_deleted_mask;

CREATE STREAM test_apply_deleted_mask(id int64, value string) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_apply_deleted_mask SELECT number, number::string FROM numbers(5);

DELETE FROM test_apply_deleted_mask WHERE id % 2 = 0;

SELECT 'Normal SELECT does not see deleted rows';
SELECT *, _row_exists FROM test_apply_deleted_mask;

SELECT 'With the setting disabled the deleted rows are visible';
SELECT *, _row_exists FROM test_apply_deleted_mask SETTINGS apply_deleted_mask = 0;

SELECT 'With the setting disabled the deleted rows are visible but still can be filterd out';
SELECT * FROM test_apply_deleted_mask WHERE _row_exists SETTINGS apply_deleted_mask = 0;

INSERT INTO test_apply_deleted_mask SELECT number, number::string FROM numbers(5, 1);

OPTIMIZE STREAM test_apply_deleted_mask FINAL SETTINGS mutations_sync=2;

SELECT 'Read the data after OPTIMIZE, all deleted rwos should be physically removed now';
SELECT *, _row_exists FROM test_apply_deleted_mask SETTINGS apply_deleted_mask = 0;

DROP STREAM test_apply_deleted_mask;
