create stream test(day Date, id uint32) engine=MergeTree partition by day order by tuple();
insert into test select to_date('2023-01-05') AS day, number from numbers(10);
with to_uint64(id) as id_with select day, count(id_with)  from test where day >= '2023-01-01' group by day limit 1000; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
