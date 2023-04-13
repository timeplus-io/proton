DROP STREAM IF EXISTS testv;

create view testv(a uint32) as select number as a from numbers(10);
select group_array(a) from testv;

DROP STREAM testv;

create view testv(a string) as select number as a from numbers(10);
select group_array(a) from testv;

DROP STREAM testv;
