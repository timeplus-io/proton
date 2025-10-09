DROP STREAM IF EXISTS 99034_test_stream;
CREATE STREAM IF NOT EXISTS 99034_test_stream(`id` int);

select sleep(2) format Null;

insert into 99034_test_stream(id, _tp_time) values(1, '2024-11-16 17:00:00.888');

select sleep(2) format Null;

--- id, _tp_time, _tp_sn
select * from table(99034_test_stream) settings asterisk_include_reserved_columns = true, asterisk_include_tp_sn_column = true;
--- id, _tp_time
select * from table(99034_test_stream) settings asterisk_include_reserved_columns = true, asterisk_include_tp_sn_column = false;
--- id, _tp_sn
select * from table(99034_test_stream) settings asterisk_include_reserved_columns = false, asterisk_include_tp_sn_column = true;
--- id
select * from table(99034_test_stream) settings asterisk_include_reserved_columns = false, asterisk_include_tp_sn_column = false;

select '========nested subquery========';
--- id, _tp_time, _tp_sn
select * from (select * from table(99034_test_stream)) settings asterisk_include_reserved_columns = true, asterisk_include_tp_sn_column = true;
--- id, _tp_time
select * from (select * from table(99034_test_stream)) settings asterisk_include_reserved_columns = true, asterisk_include_tp_sn_column = false;
--- id, _tp_sn
select * from (select * from table(99034_test_stream)) settings asterisk_include_reserved_columns = false, asterisk_include_tp_sn_column = true;
--- id
select * from (select * from table(99034_test_stream)) settings asterisk_include_reserved_columns = false, asterisk_include_tp_sn_column = false;

--- id, _tp_time, _tp_sn
select * from (select * from table(99034_test_stream)) settings asterisk_include_reserved_columns = true, asterisk_include_tp_sn_column = true;
--- id, _tp_time, _tp_sn
select * from (select *, _tp_sn from table(99034_test_stream)) settings asterisk_include_reserved_columns = true, asterisk_include_tp_sn_column = false;
--- id, _tp_sn, _tp_time
select * from (select *, _tp_time from table(99034_test_stream)) settings asterisk_include_reserved_columns = false, asterisk_include_tp_sn_column = true;
--- id, _tp_time, _tp_sn
select * from (select *, _tp_time, _tp_sn from table(99034_test_stream)) settings asterisk_include_reserved_columns = false, asterisk_include_tp_sn_column = false;

DROP STREAM 99034_test_stream;
