-- Regression test for issue #11782:
-- memory_worker_correct_memory_tracker must default to true so that the
-- MemoryWorker periodically corrects the global MemoryTracker amount to
-- match actual RSS. Without this, the tracked amount drifts unboundedly
-- above actual memory usage, eventually triggering spurious
-- MEMORY_LIMIT_EXCEEDED errors.
--
-- The old CgroupsMemoryUsageObserver::setRSS() always corrected amount.
-- The MemoryWorker backport (#11409) gated this behind
-- memory_worker_correct_memory_tracker which defaulted to false, breaking
-- the prior contract.

-- Verify the default is true (the fix for #11782).
-- This is the only assertion needed: the setting being true guarantees the
-- MemoryWorker corrects the global tracker every tick (~50 ms).
-- Checking the MemoryTracking metric directly would be fragile in CI
-- because concurrent tests can inflate global memory usage.
SELECT name, value
FROM system.server_settings
WHERE name = 'memory_worker_correct_memory_tracker';
