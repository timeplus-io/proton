-- Clean up any existing objects
DROP VIEW IF EXISTS 99033_mv;
DROP STREAM IF EXISTS 99033_v;

-- Create stream and materialized view
CREATE STREAM 99033_v(id int) settings shards=3;
CREATE MATERIALIZED VIEW 99033_mv AS SELECT * FROM 99033_v;

-- Insert test data with delays between inserts
SELECT sleep(1);
INSERT INTO 99033_v(id) VALUES (1);

SELECT sleep(1);
INSERT INTO 99033_v(id) VALUES (2);

SELECT sleep(1);
INSERT INTO 99033_v(id) VALUES (3);

SELECT sleep(1);

-- Check partition count before truncate
WITH (
    SELECT count() 
    FROM system.parts 
    WHERE table = '99033_v'
) as partition_count
SELECT 
    CASE 
        WHEN partition_count = 3 THEN 'Before truncate, partition count is equal to 3.'
        ELSE 'Unexpected partition count before truncate: ' || to_string(partition_count)
    END as status;

TRUNCATE STREAM 99033_v; -- {serverError HAVE_DEPENDENT_OBJECTS}

-- Drop view and truncate stream if conditions are met
DROP VIEW 99033_mv;

SELECT sleep(1);

TRUNCATE STREAM 99033_v;

-- Wait for background cleanup
SELECT sleep(3);

-- Check partition count after truncate
WITH (
    SELECT count() 
    FROM system.parts 
    WHERE table = '99033_v'
) as partition_count
SELECT 
    CASE 
        WHEN partition_count = 0 THEN 'After truncate, no partition found.'
        ELSE 'Unexpected partition count after truncate: ' || to_string(partition_count)
    END as status;

-- Final cleanup
DROP VIEW IF EXISTS 99033_mv;
DROP STREAM IF EXISTS 99033_v;
