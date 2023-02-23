CREATE STREAM test_table (value string) ENGINE=ExecutablePool('nonexist.py', 'TabSeparated', (foobar)); -- {serverError BAD_ARGUMENTS}
CREATE STREAM test_table (value string) ENGINE=ExecutablePool('nonexist.py', 'TabSeparated', '(SELECT 1)'); -- {serverError BAD_ARGUMENTS}
CREATE STREAM test_table (value string) ENGINE=ExecutablePool('nonexist.py', 'TabSeparated', [1,2,3]); -- {serverError BAD_ARGUMENTS}

