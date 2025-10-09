-- Stream Testing Script with Clear Output Formatting
-- This script demonstrates stream creation, schema evolution, and renaming operations

-- Cleanup existing streams
DROP STREAM IF EXISTS original_stream;
DROP STREAM IF EXISTS renamed_stream;

-- ============================================================================
-- STEP 1: Create initial stream and insert first record
-- ============================================================================
SELECT '=== STEP 1: Creating original_stream ===' AS step;

CREATE STREAM original_stream (id int, name string, val float, event_time int);
SELECT sleep(1) FORMAT Null;

INSERT INTO original_stream (id, name, val, event_time) 
VALUES(1, 's1', 11.1, 1001);
SELECT sleep(2) FORMAT Null;

SELECT '--- Initial stream content (4 columns) ---' AS info;
SELECT id, name, val, event_time 
FROM table(original_stream)
ORDER BY id;

-- ============================================================================
-- STEP 2: Add status column and insert second record
-- ============================================================================
SELECT '=== STEP 2: Adding status column ===' AS step;

ALTER STREAM original_stream ADD COLUMN IF NOT EXISTS status string;
SELECT sleep(1) FORMAT Null;

INSERT INTO original_stream (id, name, val, event_time, status) 
VALUES(2, 's2', 22.2, 1002, 'active');
SELECT sleep(1) FORMAT Null;

SELECT '--- Stream content after adding status column (5 columns) ---' AS info;
SELECT id, name, val, event_time, status 
FROM table(original_stream)
ORDER BY id;

-- ============================================================================
-- STEP 3: Rename stream and insert third record
-- ============================================================================
SELECT '=== STEP 3: Renaming stream ===' AS step;

RENAME STREAM original_stream TO renamed_stream;
SELECT sleep(1) FORMAT Null;

INSERT INTO renamed_stream (id, name, val, event_time, status) 
VALUES(3, 's3', 33.3, 1003, 'pending');
SELECT sleep(1) FORMAT Null;

SELECT '--- Renamed stream content (5 columns) ---' AS info;
SELECT id, name, val, event_time, status 
FROM table(renamed_stream)
ORDER BY id;

-- ============================================================================
-- STEP 4: Add priority column and show current state
-- ============================================================================
SELECT '=== STEP 4: Adding priority column ===' AS step;

ALTER STREAM renamed_stream ADD COLUMN IF NOT EXISTS priority int;
SELECT sleep(1) FORMAT Null;

SELECT '--- Stream content after adding priority column (6 columns) ---' AS info;
SELECT id, name, val, event_time, status, priority 
FROM table(renamed_stream)
ORDER BY id;

INSERT INTO renamed_stream (id, name, val, event_time, status, priority) 
VALUES(4, 's4', 44.4, 1004, 'completed', 1);
SELECT sleep(1) FORMAT Null;

-- ============================================================================
-- STEP 5: Rename column and insert new record
-- ============================================================================
SELECT '=== STEP 5: Renaming event_time to time_value ===' AS step;

ALTER STREAM renamed_stream RENAME COLUMN IF EXISTS event_time to time_value;
SELECT sleep(1) FORMAT Null;

SELECT '--- Stream content after column rename (6 columns) ---' AS info;
SELECT id, name, val, time_value, status, priority 
FROM table(renamed_stream)
ORDER BY id;

INSERT INTO renamed_stream (id, name, val, time_value, status, priority) 
VALUES(5, 's5', 55.5, 1005, 'failed', 2);
SELECT sleep(1) FORMAT Null;

-- ============================================================================
-- STEP 6: Add final column and complete testing
-- ============================================================================
SELECT '=== STEP 6: Adding category column ===' AS step;

ALTER STREAM renamed_stream ADD COLUMN IF NOT EXISTS category string;
SELECT sleep(1) FORMAT Null;

INSERT INTO renamed_stream (id, name, val, time_value, status, priority, category) 
VALUES(6, 's6', 66.6, 1006, 'processing', 3, 'important');
SELECT sleep(1) FORMAT Null;

SELECT '--- FINAL: Complete stream content (7 columns) ---' AS info;
SELECT id, name, val, time_value, status, priority, category 
FROM table(renamed_stream)
ORDER BY id;
