SET query_mode = 'table';
drop stream IF EXISTS testv;

create view testv(a uint32) as select number a from numbers(10);
select group_array(a) from testv;

drop stream testv;

create view testv(a string) as select number a from numbers(10);
select group_array(a) from testv;

drop stream testv;
