drop view if exists pv;
drop stream if exists table_pv;
create stream table_pv (id int32, timestamp_field datetime);
select sleep(1) FORMAT Null;

insert into table_pv(id, timestamp_field) values(1, '2024-03-01 00:00:00');
insert into table_pv(id, timestamp_field) values (2, '2024-04-01 01:00:00');

create view pv as select * from table_pv where timestamp_field > {timestamp_param:datetime};
select sleep(1) FORMAT Null;

SET query_mode='table';
select id, timestamp_field from pv (timestamp_param=to_datetime('2024-04-01 00:00:01'));

select id, timestamp_field from pv (timestamp_param=to_datetime('2024-040')); -- { serverError CANNOT_PARSE_DATETIME }

drop stream table_pv;
drop view pv;
