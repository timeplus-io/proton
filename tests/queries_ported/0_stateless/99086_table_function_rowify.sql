drop stream if exists 99086_stream;
create stream 99086_stream(id int) settings flush_threshold_count=1;

select sleep(2) format Null;
insert into 99086_stream(id) values (1), (2), (3);

select sleep(2) format Null;
select id, block_size() from rowify(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select id, block_size() from rowify(99086_stream, id); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select id, block_size() from rowify(99086_stream) limit 3 settings seek_to='earliest';
with cte as (select id from 99086_stream settings seek_to='earliest') select id, block_size() from rowify(cte) limit 3;

select id, block_size() from table(rowify(99086_stream)); -- { serverError UNSUPPORTED }
select id, block_size() from rowify(table(99086_stream));

drop stream if exists 99086_stream;
