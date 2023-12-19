SET query_mode = 'table';
drop stream if exists test_02815;
create stream if not exists test_02815(id int, value int);
insert into test_02815(id, value) values (1, 4);
insert into test_02815(id, value) values (2, 4);
insert into test_02815(id, value) values (1, 2);
insert into test_02815(id, value) values (2, 7);
insert into test_02815(id, value) values (3, 2);
insert into test_02815(id, value) values (3, 10);
select sleep(3);
select '---- normal aggregate functions ----';
select id, COUNT(*), MAX(value), MIN(value), AVG(value), SUM(value), VAR_SAMP(value), to_String(id) from test_02815 group by id;

select '---- combination aggrefation functions ----';

select id, COUNT_IF(value > 3), SUM_DISTINCT(value) from test_02815 group by id;
drop stream if exists test_02815;
