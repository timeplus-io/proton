EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data string');
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data string', SETTINGS max_command_execution_time=100);
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data string', SETTINGS max_command_execution_time=100, command_read_timeout=1);
