CREATE DATABASE demo
SETTINGS
    type = 'iceberg',
    catalog_uri = 'http://iceberg-rest:8181',
    catalog_type = 'rest',
    warehouse = 's3://warehouse/',
    storage_endpoint = 'http://minio:9000',
    rest_catalog_sigv4_enabled = false,
    rest_catalog_signing_region = 'us-east-1',
    use_environment_credentials = false,
    storage_credential = 'admin:Password!';


-- List namespaces
SHOW DATABASES;

-- List tables in demo namespace
SHOW TABLES FROM demo;

show create demo.events

-- Query the events table we created in Jupyter
SELECT * FROM demo.events;

-- Insert more data into the events table
INSERT INTO demo.events (id, timestamp, user_id, event_type, value) 
VALUES (11, now64(6), 'user_1', 'login', NULL);

INSERT INTO demo.events (id, timestamp, user_id, event_type, value) 
VALUES (12, now64(6), 'user_2', 'login', NULL);