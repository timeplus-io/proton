SET query_mode = 'table';
drop stream if exists fooL;
drop stream if exists fooR;
create stream fooL (a int32, v string) engine = Memory;
create stream fooR (a int32, v string) engine = Memory;

insert into fooL select number, 'L'  || to_string(number) from numbers(2);
insert into fooL select number, 'LL' || to_string(number) from numbers(2);
insert into fooR select number, 'R'  || to_string(number) from numbers(2);

select distinct a from fooL semi left join fooR using(a) order by a;

drop stream fooL;
drop stream fooR;
