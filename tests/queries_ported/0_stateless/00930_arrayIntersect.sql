SET query_mode = 'table';
drop stream if exists array_intersect;

create stream array_intersect (date date, arr array(uint8)) engine=MergeTree partition by date order by date;

insert into array_intersect values ('2019-01-01', [1,2,3]);
insert into array_intersect values ('2019-01-01', [1,2]);
insert into array_intersect values ('2019-01-01', [1]);
insert into array_intersect values ('2019-01-01', []);

select array_sort(array_intersect(arr, [1,2])) from array_intersect order by arr;
select array_sort(array_intersect(arr, [])) from array_intersect order by arr;
select array_sort(array_intersect([], arr)) from array_intersect order by arr;
select array_sort(array_intersect([1,2], arr)) from array_intersect order by arr;
select array_sort(array_intersect([1,2], [1,2,3,4])) from array_intersect order by arr;
select array_sort(array_intersect([], [])) from array_intersect order by arr;

optimize table array_intersect;

select array_sort(array_intersect(arr, [1,2])) from array_intersect order by arr;
select array_sort(array_intersect(arr, [])) from array_intersect order by arr;
select array_sort(array_intersect([], arr)) from array_intersect order by arr;
select array_sort(array_intersect([1,2], arr)) from array_intersect order by arr;
select array_sort(array_intersect([1,2], [1,2,3,4])) from array_intersect order by arr;
select array_sort(array_intersect([], [])) from array_intersect order by arr;

drop stream if exists array_intersect;

select '-';
select array_sort(array_intersect([-100], [156]));
select array_sort(array_intersect([1], [257]));
