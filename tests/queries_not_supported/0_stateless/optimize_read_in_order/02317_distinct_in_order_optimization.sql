select '-- enable distinct in order optimization';
set optimize_distinct_in_order=1;
select '-- create stream with only primary key columns';
drop stream if exists distinct_in_order sync;
create stream distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
select '-- the same values in every chunk, pre-distinct should skip entire chunks with the same key as previous one';
insert into distinct_in_order (a) select * from zeros(10);
insert into distinct_in_order (a) select * from zeros(10); -- this entire chunk should be skipped in pre-distinct
select distinct * from distinct_in_order settings max_block_size=10, max_threads=1;

select '-- create stream with only primary key columns';
select '-- pre-distinct should skip part of chunk since it contains values from previous one';
drop stream if exists distinct_in_order sync;
create stream distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
insert into distinct_in_order (a) select * from zeros(10);
insert into distinct_in_order select * from numbers(10); -- first row (0) from this chunk should be skipped in pre-distinct
select distinct a from distinct_in_order settings max_block_size=10, max_threads=1;

select '-- create stream with not only primary key columns';
drop stream if exists distinct_in_order sync;
create stream distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b);
insert into distinct_in_order select number % number, number % 5, number % 10 from numbers(1,1000000);

select '-- distinct with primary key prefix only';
select distinct a from distinct_in_order;
select '-- distinct with primary key prefix only, order by sorted column';
select distinct a from distinct_in_order order by a;
select '-- distinct with primary key prefix only, order by sorted column desc';
select distinct a from distinct_in_order order by a desc;

select '-- distinct with full key, order by sorted column';
select distinct a,b from distinct_in_order order by b;
select '-- distinct with full key, order by sorted column desc';
select distinct a,b from distinct_in_order order by b desc;

select '-- distinct with key prefix and non-sorted column, order by non-sorted';
select distinct a,c from distinct_in_order order by c;
select '-- distinct with key prefix and non-sorted column, order by non-sorted desc';
select distinct a,c from distinct_in_order order by c desc;

select '-- distinct with non-key prefix and non-sorted column, order by non-sorted';
select distinct b,c from distinct_in_order order by c;
select '-- distinct with non-key prefix and non-sorted column, order by non-sorted desc';
select distinct b,c from distinct_in_order order by c desc;

select '-- distinct with constants columns';
-- { echoOn }
select distinct 1 as x, 2 as y from distinct_in_order;
select distinct 1 as x, 2 as y from distinct_in_order order by x;
select distinct 1 as x, 2 as y from distinct_in_order order by x, y;
select a, x from (select distinct a, 1 as x from distinct_in_order order by x) order by a;
select distinct a, 1 as x, 2 as y from distinct_in_order order by a;
select a, b, x, y from(select distinct a, b, 1 as x, 2 as y from distinct_in_order order by a) order by a, b;
select distinct x, y from (select 1 as x, 2 as y from distinct_in_order order by x) order by y;
select distinct a, b, x, y from (select a, b, 1 as x, 2 as y from distinct_in_order order by a) order by a, b;
-- { echoOff }

drop stream if exists distinct_in_order sync;

select '-- check that distinct in order returns the same result as ordinary distinct';
drop stream if exists distinct_cardinality_low sync;
CREATE STREAM distinct_cardinality_low (low uint64, medium uint64, high uint64) ENGINE MergeTree() ORDER BY (low, medium);
INSERT INTO distinct_cardinality_low SELECT number % 1e1, number % 1e2, number % 1e3 FROM numbers_mt(1e4);

drop stream if exists distinct_in_order sync;
drop stream if exists ordinary_distinct sync;

select '-- check that distinct in order WITH order by returns the same result as ordinary distinct';
create stream distinct_in_order (low uint64, medium uint64, high uint64) engine=MergeTree() order by (low, medium);
insert into distinct_in_order select distinct * from distinct_cardinality_low order by high settings optimize_distinct_in_order=1;
create stream ordinary_distinct (low uint64, medium uint64, high uint64) engine=MergeTree() order by (low, medium);
insert into ordinary_distinct select distinct * from distinct_cardinality_low order by high settings optimize_distinct_in_order=0;
select count() as diff from (select distinct * from distinct_in_order except select * from ordinary_distinct);

drop stream if exists distinct_in_order sync;
drop stream if exists ordinary_distinct sync;

select '-- check that distinct in order WITHOUT order by returns the same result as ordinary distinct';
create stream distinct_in_order (low uint64, medium uint64, high uint64) engine=MergeTree() order by (low, medium);
insert into distinct_in_order select distinct * from distinct_cardinality_low settings optimize_distinct_in_order=1;
create stream ordinary_distinct (low uint64, medium uint64, high uint64) engine=MergeTree() order by (low, medium);
insert into ordinary_distinct select distinct * from distinct_cardinality_low settings optimize_distinct_in_order=0;
select count() as diff from (select distinct * from distinct_in_order except select * from ordinary_distinct);

drop stream if exists distinct_in_order;
drop stream if exists ordinary_distinct;

select '-- check that distinct in order WITHOUT order by and WITH filter returns the same result as ordinary distinct';
create stream distinct_in_order (low uint64, medium uint64, high uint64) engine=MergeTree() order by (low, medium);
insert into distinct_in_order select distinct * from distinct_cardinality_low where low > 0 settings optimize_distinct_in_order=1;
create stream ordinary_distinct (low uint64, medium uint64, high uint64) engine=MergeTree() order by (low, medium);
insert into ordinary_distinct select distinct * from distinct_cardinality_low where low > 0 settings optimize_distinct_in_order=0;
select count() as diff from (select distinct * from distinct_in_order except select * from ordinary_distinct);

drop stream if exists distinct_in_order;
drop stream if exists ordinary_distinct;
drop stream if exists distinct_cardinality_low;

-- bug 42185
drop stream if exists sorting_key_empty_tuple;
drop stream if exists sorting_key_contain_function;

select '-- bug 42185, distinct in order and empty sort description';
select '-- distinct in order, sorting key tuple()';
create stream sorting_key_empty_tuple (a int, b int) engine=MergeTree() order by tuple();
insert into sorting_key_empty_tuple select number % 2, number % 5 from numbers(1,10);
select distinct a from sorting_key_empty_tuple;

select '-- distinct in order, sorting key contains function';
create stream sorting_key_contain_function (datetime DateTime, a int) engine=MergeTree() order by (to_date(datetime));
insert into sorting_key_contain_function values ('2000-01-01', 1);
insert into sorting_key_contain_function values ('2000-01-01', 2);
select distinct datetime from sorting_key_contain_function;
select distinct to_date(datetime) from sorting_key_contain_function;

drop stream sorting_key_empty_tuple;
drop stream sorting_key_contain_function;
