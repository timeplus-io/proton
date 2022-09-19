SET query_mode = 'table';
drop stream if exists defaulted;

create stream defaulted (col1 default 0) engine=Memory;
desc stream defaulted;
drop stream defaulted;

create stream defaulted (col1 uint32, col2 default col1 + 1, col3 materialized col1 + 2, col4 alias col1 + 3) engine=Memory;
desc stream defaulted;
insert into defaulted (col1) values (10);
select * from defaulted;
select col3, col4 from defaulted;
drop stream defaulted;

create stream defaulted (col1 int8, col2 uint64 default (SELECT dummy+99 from system.one)) engine=Memory; --{serverError 116}

create stream defaulted (payload string, date materialized today(), key materialized 0 * rand()) engine=MergeTree(date, key, 8192);
desc stream defaulted;
insert into defaulted (payload) values ('hello clickhouse');
select * from defaulted;
alter stream defaulted add column payload_length uint64 materialized length(payload);
desc stream defaulted;
select *, payload_length from defaulted;
insert into defaulted (payload) values ('some string');
select *, payload_length from defaulted order by payload;
select *, payload_length from defaulted order by payload;
alter stream defaulted modify column payload_length default length(payload);
desc stream defaulted;
select * from defaulted order by payload;
alter stream defaulted modify column payload_length default length(payload) % 65535;
desc stream defaulted;
select * from defaulted order by payload;
alter stream defaulted modify column payload_length uint16 default length(payload);
desc stream defaulted;
alter stream defaulted drop column payload_length;
desc stream defaulted;
select * from defaulted order by payload;
drop stream defaulted;
