drop stream if exists user_events;
-- 1. Create a table with a primary key, sort order, and minmax index directly in DDL
CREATE STREAM user_events (
    user_id uint64,
    event_type string,
    event_date Date,
    event_time DateTime,
    payload string,
    INDEX event_type_idx event_type TYPE minmax GRANULARITY 4
)
PARTITION BY to_YYYYMM(event_date)
ORDER BY (user_id, event_time)
SETTINGS index_granularity = 8192;


SELECT sleep(2) FORMAT Null;

-- 2. Insert sample data to populate the index
INSERT INTO user_events (user_id, event_type, event_date, event_time, payload)
SELECT
    number % 1000,
    ['click', 'impression', 'purchase', 'signup'][number % 4],
    to_date('2025-05-08') - to_interval_day(number%30),
    to_datetime('2025-05-08T08:23:58') - to_Interval_Second(number % 86400),
    '{"value": ' || to_string(number) || ', "metadata": {"source": "test"}}'
FROM numbers(10);

SELECT sleep(2) FORMAT Null;

-- 3. Verify the index is being used with EXPLAIN
EXPLAIN indexes = 1
SELECT count() FROM table(user_events) WHERE event_type = 'purchase';
-- Output should show that event_type_idx is used


show create user_events;


select 'Now rename the column `event_type` to `action_name`';
-- 4. Now rename the column that has the index
ALTER STREAM user_events RENAME COLUMN event_type TO action_name;
SELECT sleep(2) FORMAT Null;

show create user_events;

-- 5. Verify the index still works after renaming
EXPLAIN indexes = 1
SELECT count() FROM table(user_events) WHERE action_name = 'purchase';
-- Output should show the index is still being used, just with the new column name

-- 6. Insert more data with the new column name
INSERT INTO user_events (user_id, action_name, event_date, event_time, payload)
SELECT
    number % 1000,
    ['click', 'impression', 'purchase', 'signup'][number % 4],
    to_date('2025-05-08') - to_interval_day(number%30),
    to_datetime('2025-05-08T08:23:58') - to_Interval_Second(number % 86400),
    '{"value": ' || to_string(number) || ', "metadata": {"source": "batch2"}}'
FROM numbers(50);
SELECT sleep(2) FORMAT Null;

-- 7. Query the data including both old and new inserts
SELECT 
    action_name,
    count(),
    min(event_date) as earliest_date,
    max(event_date) as latest_date
FROM table(user_events)
WHERE action_name = 'purchase'
GROUP BY action_name;