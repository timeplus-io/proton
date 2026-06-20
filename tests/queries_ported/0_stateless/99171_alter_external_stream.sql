-- Test ALTER EXTERNAL STREAM comment and settings

DROP STREAM IF EXISTS test_alter_external_stream;

CREATE EXTERNAL STREAM test_alter_external_stream (val int32) AS $$
def read_99171():
    return [(1,)]
$$ SETTINGS type = 'python', mode = 'batch', read_function_name = 'read_99171';

-- Show create stream to verify initial comment and settings
SHOW CREATE test_alter_external_stream;

-- Test ALTER EXTERNAL STREAM SET COMMENT
ALTER STREAM test_alter_external_stream MODIFY COMMENT 'Comment v1';
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY COMMENT 'Comment v2';
SHOW CREATE test_alter_external_stream;

-- Test ALTER EXTERNAL STREAM MODIFY SETTING
ALTER STREAM test_alter_external_stream MODIFY SETTING type = 'kafka'; -- { serverError UNSUPPORTED }
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY SETTING mode = 'streaming';
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY COMMENT 'Comment v3';
SHOW CREATE test_alter_external_stream;

-- Drop stream
DROP STREAM test_alter_external_stream;
