
SET query_mode = 'table';
drop stream if exists prewhere_column_missing;

create stream prewhere_column_missing (d date default '2015-01-01', x uint64) engine=MergeTree(d, x, 1);

insert into prewhere_column_missing (x) values (0);
select * from prewhere_column_missing;

alter stream prewhere_column_missing add column arr array(uint64);
select * from prewhere_column_missing;

select *, array_sum(arr) as s from prewhere_column_missing;
select *, array_sum(arr) as s from prewhere_column_missing where s = 0;
select *, array_sum(arr) as s from prewhere_column_missing prewhere s = 0;

select *, length(arr) as l from prewhere_column_missing;
select *, length(arr) as l from prewhere_column_missing where l = 0;
select *, length(arr) as l from prewhere_column_missing prewhere l = 0;

alter stream prewhere_column_missing add column hash_x uint64 default intHash64(x);

select * from prewhere_column_missing;
select * from prewhere_column_missing where hash_x = intHash64(x);
select * from prewhere_column_missing prewhere hash_x = intHash64(x);
select * from prewhere_column_missing where hash_x = intHash64(x) and length(arr) = 0;
select * from prewhere_column_missing prewhere hash_x = intHash64(x) and length(arr) = 0;
select * from prewhere_column_missing where hash_x = intHash64(x) and length(arr) = 0 and array_sum(arr) = 0;
select * from prewhere_column_missing prewhere hash_x = intHash64(x) and length(arr) = 0 and array_sum(arr) = 0;

drop stream prewhere_column_missing;
