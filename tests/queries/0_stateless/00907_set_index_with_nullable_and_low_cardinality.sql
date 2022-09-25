SET query_mode = 'table';
drop stream if exists nullable_set_index;
create stream nullable_set_index (a uint64, b nullable(string), INDEX b_index b TYPE set(0) GRANULARITY 8192) engine = MergeTree order by a;
insert into nullable_set_index values (1, 'a');
insert into nullable_set_index values (2, 'b');
select * from nullable_set_index where b = 'a';
select '-';
select * from nullable_set_index where b = 'b';
select '-';
select * from nullable_set_index where b = 'c';
select '--';

drop stream if exists nullable_set_index;
create stream nullable_set_index (a uint64, b nullable(string), INDEX b_index b TYPE set(1) GRANULARITY 8192) engine = MergeTree order by a;
insert into nullable_set_index values (1, 'a');
insert into nullable_set_index values (2, 'b');
select * from nullable_set_index where b = 'a';
select '-';
select * from nullable_set_index where b = 'b';
select '-';
select * from nullable_set_index where b = 'c';
select '--';

drop stream if exists nullable_set_index;
create stream nullable_set_index (a uint64, b nullable(string), INDEX b_index b TYPE set(0) GRANULARITY 8192) engine = MergeTree order by a;
insert into nullable_set_index values (1, 'a'), (2, 'b');
select * from nullable_set_index where b = 'a';
select '-';
select * from nullable_set_index where b = 'b';
select '-';
select * from nullable_set_index where b = 'c';
select '----';


drop stream if exists nullable_set_index;
create stream nullable_set_index (a uint64, b low_cardinality(nullable(string)), INDEX b_index b TYPE set(0) GRANULARITY 8192) engine = MergeTree order by a;
insert into nullable_set_index values (1, 'a');
insert into nullable_set_index values (2, 'b');
select * from nullable_set_index where b = 'a';
select '-';
select * from nullable_set_index where b = 'b';
select '-';
select * from nullable_set_index where b = 'c';
select '--';

drop stream if exists nullable_set_index;
create stream nullable_set_index (a uint64, b low_cardinality(nullable(string)), INDEX b_index b TYPE set(1) GRANULARITY 8192) engine = MergeTree order by a;
insert into nullable_set_index values (1, 'a');
insert into nullable_set_index values (2, 'b');
select * from nullable_set_index where b = 'a';
select '-';
select * from nullable_set_index where b = 'b';
select '-';
select * from nullable_set_index where b = 'c';
select '--';

drop stream if exists nullable_set_index;
create stream nullable_set_index (a uint64, b low_cardinality(nullable(string)), INDEX b_index b TYPE set(0) GRANULARITY 8192) engine = MergeTree order by a;
insert into nullable_set_index values (1, 'a'), (2, 'b');
select * from nullable_set_index where b = 'a';
select '-';
select * from nullable_set_index where b = 'b';
select '-';
select * from nullable_set_index where b = 'c';
select '----';

drop stream if exists nullable_set_index;

