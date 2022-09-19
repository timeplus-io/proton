-- Tags: no-fasttest, use-hdfs
SET query_mode = 'table';
drop stream if exists test_table_hdfs_syntax
;
create stream test_table_hdfs_syntax (id uint32) ENGINE = HDFS('')
; -- { serverError 36 }
create stream test_table_hdfs_syntax (id uint32) ENGINE = HDFS('','','', '')
; -- { serverError 42 }
drop stream if exists test_table_hdfs_syntax
;
