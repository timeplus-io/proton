SET query_mode = 'table';
drop stream if exists test_table_url_syntax
;
create stream test_table_url_syntax (id uint32) ENGINE = URL('')
; -- { serverError 36 }
create stream test_table_url_syntax (id uint32) ENGINE = URL('','','','')
; -- { serverError 42 }
drop stream if exists test_table_url_syntax
;
