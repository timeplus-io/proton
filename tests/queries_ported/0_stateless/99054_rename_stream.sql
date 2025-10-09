-- Setup
DROP DATABASE IF EXISTS test_rename_db CASCADE;
DROP DATABASE IF EXISTS test_rename_db2 CASCADE;

select sleep(1) FORMAT Null;
CREATE DATABASE test_rename_db;
select sleep(1) FORMAT Null;

-- Create test streams
CREATE STREAM test_rename_db.original_stream (id int32, data string, timestamp datetime);
select sleep(1) FORMAT Null;
CREATE STREAM test_rename_db.target_exists_stream (id int32, data string);
select sleep(1) FORMAT Null;

-- Insert some test data
INSERT INTO test_rename_db.original_stream(id, data, timestamp) VALUES (1, 'test data', '2025-01-01 20:00:01');

-- Test basic rename
RENAME STREAM test_rename_db.original_stream TO test_rename_db.renamed_stream;
select sleep(1) FORMAT Null;

-- Verify the renamed stream exists and has the data
SELECT id, data, timestamp FROM table(test_rename_db.renamed_stream);

-- Verify the original stream doesn't exist anymore
SELECT * FROM table(test_rename_db.original_stream); -- { serverError UNKNOWN_STREAM }

-- Try to rename to existing stream (should fail)
RENAME STREAM test_rename_db.renamed_stream TO test_rename_db.target_exists_stream; -- { serverError STREAM_ALREADY_EXISTS }
select sleep(1) FORMAT Null;

-- Test cross-database rename (should fail)
DROP DATABASE IF EXISTS test_rename_db2;
CREATE DATABASE test_rename_db2;
select sleep(1) FORMAT Null;
RENAME STREAM test_rename_db.renamed_stream TO test_rename_db2.cross_db_stream; -- { serverError NOT_IMPLEMENTED }
select sleep(1) FORMAT Null;

-- Test renaming with dependencies
CREATE MATERIALIZED VIEW test_rename_db.dependent_view
AS SELECT id, data FROM test_rename_db.renamed_stream;
select sleep(1) FORMAT Null;

-- Try renaming stream with dependency (implementation-dependent, might fail)
RENAME STREAM test_rename_db.renamed_stream TO test_rename_db.with_dependencies; -- { serverError HAVE_DEPENDENT_OBJECTS }
select sleep(1) FORMAT Null;

-- Cleanup
DROP DATABASE IF EXISTS test_rename_db CASCADE;
DROP DATABASE IF EXISTS test_rename_db2 CASCADE;
