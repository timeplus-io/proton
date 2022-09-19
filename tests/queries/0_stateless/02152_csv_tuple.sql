SET query_mode = 'table';
drop stream if exists test_02152;
create stream test_02152 (x uint32, y string, z array(uint32), t tuple(uint32, string, array(uint32))) engine=File('CSV') settings format_csv_delimiter=';';
insert into test_02152 select 1, 'Hello', [1,2,3], tuple(2, 'World', [4,5,6]); 
select * from test_02152;
drop stream test_02152;

create stream test_02152 (x uint32, y string, z array(uint32), t tuple(uint32, string, array(uint32))) engine=File('CustomSeparated') settings format_custom_field_delimiter='<field_delimiter>', format_custom_row_before_delimiter='<row_start>', format_custom_row_after_delimiter='<row_end_delimiter>', format_custom_escaping_rule='CSV';
insert into test_02152 select 1, 'Hello', [1,2,3], tuple(2, 'World', [4,5,6]);
select * from test_02152;
drop stream test_02152;

