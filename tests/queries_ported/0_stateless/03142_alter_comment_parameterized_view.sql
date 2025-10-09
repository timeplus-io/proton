DROP VIEW IF EXISTS test_table_comment;
CREATE VIEW test_table_comment AS SELECT to_string({date_from:string});
ALTER VIEW test_table_comment MODIFY COMMENT 'test comment';
SELECT create_table_query FROM system.tables WHERE name = 'test_table_comment' AND database = current_database();
DROP VIEW test_table_comment;
