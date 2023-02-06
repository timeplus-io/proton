-- Tags: no-fasttest
insert into stream function file('data.jsonl', 'JSONEachRow', 'x uint32') select * from numbers(10);
select * from file('data.jsonl');
