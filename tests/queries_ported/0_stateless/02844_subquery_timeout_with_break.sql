-- Verify timeout_overflow_mode='break' doesn't throw TOO_SLOW error
-- The fix ensures throttle() respects overflow mode when estimating execution time
SET timeout_overflow_mode='break';
SET max_execution_time=0.001;
SELECT count() FROM numbers(100);
SELECT 'ok';
