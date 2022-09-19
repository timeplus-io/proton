SET query_mode = 'table';
drop stream if exists array_intersect;

create stream array_intersect (date date, arr array(uint8)) engine=MergeTree partition by date order by date;

insert into array_intersect values ('2019-01-01', [1,2,3]);
insert into array_intersect values ('2019-01-01', [1,2]);
insert into array_intersect values ('2019-01-01', [1]);
insert into array_intersect values ('2019-01-01', []);

select arraySort(arrayIntersect(arr, [1,2])) from array_intersect order by arr;
select arraySort(arrayIntersect(arr, [])) from array_intersect order by arr;
select arraySort(arrayIntersect([], arr)) from array_intersect order by arr;
select arraySort(arrayIntersect([1,2], arr)) from array_intersect order by arr;
select arraySort(arrayIntersect([1,2], [1,2,3,4])) from array_intersect order by arr;
select arraySort(arrayIntersect([], [])) from array_intersect order by arr;

optimize table array_intersect;

select arraySort(arrayIntersect(arr, [1,2])) from array_intersect order by arr;
select arraySort(arrayIntersect(arr, [])) from array_intersect order by arr;
select arraySort(arrayIntersect([], arr)) from array_intersect order by arr;
select arraySort(arrayIntersect([1,2], arr)) from array_intersect order by arr;
select arraySort(arrayIntersect([1,2], [1,2,3,4])) from array_intersect order by arr;
select arraySort(arrayIntersect([], [])) from array_intersect order by arr;

drop stream if exists array_intersect;

select '-';
select arraySort(arrayIntersect([-100], [156]));
select arraySort(arrayIntersect([1], [257]));
