-- Tags: no-fasttest, use-hdfs

drop stream if exists test_table_hdfs_syntax
;
create stream test_table_hdfs_syntax (id UInt32) ENGINE = HDFS('')
; -- { serverError 36 }
create stream test_table_hdfs_syntax (id UInt32) ENGINE = HDFS('','','', '')
; -- { serverError 42 }
drop stream if exists test_table_hdfs_syntax
;
