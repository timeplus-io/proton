-- Test task validation at creation time

-- Test 1: Syntax error in task query
CREATE TASK test_task_syntax_err SCHEDULE INTERVAL 1 HOUR AS SELECT * FORM non_existent; -- { serverError SYNTAX_ERROR }

-- Test 2: Non-existent target stream
CREATE TASK test_task_no_target SCHEDULE INTERVAL 1 HOUR INTO non_existent_stream AS SELECT 1; -- { serverError UNKNOWN_STREAM }

-- Test 3: Schema mismatch
CREATE STREAM target_stream_mismatch (a int, b string);
CREATE TASK test_task_mismatch SCHEDULE INTERVAL 1 HOUR INTO target_stream_mismatch AS SELECT 1 as c; -- { serverError INCORRECT_QUERY }
DROP STREAM target_stream_mismatch;

-- Test 4: Valid task creation with matching schema
CREATE STREAM target_stream_valid (a int);
CREATE TASK test_task_valid SCHEDULE INTERVAL 1 HOUR INTO target_stream_valid AS SELECT 1 as a;
DROP TASK test_task_valid;
DROP STREAM target_stream_valid;

-- Test 5: Valid task creation without target stream
CREATE TASK test_task_no_target_valid SCHEDULE INTERVAL 1 HOUR AS SELECT 1;
DROP TASK test_task_no_target_valid;

-- Test 6: Valid task with checkpoint placeholder expanded
CREATE TASK test_task_checkpoint SCHEDULE INTERVAL 1 HOUR CHECKPOINT '{"last_id": "0"}' AS SELECT * FROM (SELECT 1 as id) WHERE id > ${last_id};
DROP TASK test_task_checkpoint;

-- Test 7: Invalid checkpoint JSON
-- CREATE TASK test_task_bad_checkpoint SCHEDULE INTERVAL 1 HOUR CHECKPOINT 'invalid_json' AS SELECT 1; -- { clientError BAD_ARGUMENTS }

-- Test 8: Type incompatibility (cannot cast array to int)
CREATE STREAM target_stream_type_mismatch (a int);
CREATE TASK test_task_type_mismatch SCHEDULE INTERVAL 1 HOUR INTO target_stream_type_mismatch AS SELECT [1,2,3] as a; -- { serverError BAD_ARGUMENTS }
DROP STREAM target_stream_type_mismatch;

-- Test 9: Valid type conversion (int to string is allowed)
CREATE STREAM target_stream_type_convert (a string);
CREATE TASK test_task_type_convert SCHEDULE INTERVAL 1 HOUR INTO target_stream_type_convert AS SELECT 1 as a;
DROP TASK test_task_type_convert;
DROP STREAM target_stream_type_convert;