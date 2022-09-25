DROP STREAM IF EXISTS table_with_single_pk;

create stream table_with_single_pk
(
  key uint8,
  value string
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO table_with_single_pk SELECT number, to_string(number % 10) FROM numbers(1000000);

-- Check NewPart
SYSTEM FLUSH LOGS;
WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE table = 'table_with_single_pk' AND database = currentDatabase() AND event_type = 'NewPart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
SELECT if(date_diff('second', to_datetime(time.2), to_datetime(time.1)) = 0, 'ok', 'fail');

-- Now let's check RemovePart
TRUNCATE TABLE table_with_single_pk;
SYSTEM FLUSH LOGS;
WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE table = 'table_with_single_pk' AND database = currentDatabase() AND event_type = 'RemovePart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
SELECT if(date_diff('second', to_datetime(time.2), to_datetime(time.1)) = 0, 'ok', 'fail');

DROP STREAM table_with_single_pk;
