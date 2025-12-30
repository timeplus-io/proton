CREATE DATABASE demo
SETTINGS
    type = 'iceberg',
    catalog_uri = 'http://iceberg-rest:8181',
    catalog_type = 'rest',
    warehouse = 's3://warehouse/',
    -- Keep s3:// here: HTTP endpoints are rewritten to s3://<host>/... and would target the wrong bucket.
    storage_endpoint = 's3://warehouse/',
    rest_catalog_sigv4_enabled = false,
    rest_catalog_signing_region = 'us-east-1',
    use_environment_credentials = false,
    storage_credential = 'admin:Password!';


-- List namespaces
SHOW DATABASES;

-- List tables existingin demo namespace
SHOW TABLES FROM demo;

SHOW CREATE demo.events;

-- Query the events table we created in Jupyter
SELECT * FROM demo.events;

-- Insert more data into the events table
INSERT INTO demo.events (id, timestamp, user_id, event_type, value) 
VALUES (11, now64(6), 'user_1', 'login', NULL);

INSERT INTO demo.events (id, timestamp, user_id, event_type, value) 
VALUES (12, now64(6), 'user_2', 'login', NULL);

-- Create new table from proton
CREATE STREAM IF NOT EXISTS demo.proton_events
(
    id int64,
    timestamp datetime64(6, 'UTC'),
    user_id string,
    event_type string,
    value nullable(float64)
);

INSERT INTO demo.proton_events (id, timestamp, user_id, event_type, value)
VALUES (101, now64(6), 'user_9', 'login', NULL);

SELECT id, timestamp, user_id, event_type, value FROM demo.proton_events;
