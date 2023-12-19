drop stream if exists test_02815;
create stream if not exists test_02815(id int, value int) engine=Memory;
insert into test_02815(id, value) values (1, 4);
insert into test_02815(id, value) values (2, 3);
insert into test_02815(id, value) values (1, 2);
insert into test_02815(id, value) values (2, 7);
-- all function are case insensitive
select COUNT(*), MAX(value), MIN(value), AVG(value), SUM(value), VAR_SAMP(value), to_String(id) from test_02815 group by id;
drop stream if exists test_02815;