-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Look at DistributedFilesToInsert, so cannot run in parallel.

-- otherwise SYSTEM STOP DISTRIBUTED SENDS does not makes any effect (for localhost)
-- (i.e. no .bin files and hence no sending is required)
set prefer_localhost_replica=0;

SET query_mode = 'table';
drop stream if exists data_01460;
drop stream if exists dist_01460;

create stream data_01460 as system.one engine=Null();
create stream dist_01460 as data_01460 engine=Distributed(test_shard_localhost, currentDatabase(), data_01460) settings monitor_sleep_time_ms=50;

select 'INSERT';
select value from system.metrics where metric = 'DistributedFilesToInsert';
insert into dist_01460 select * from system.one;
select sleep(1) format Null; -- monitor_sleep_time_ms
select value from system.metrics where metric = 'DistributedFilesToInsert';

select 'STOP/START DISTRIBUTED SENDS';
system stop distributed sends dist_01460;
insert into dist_01460 select * from system.one;
select sleep(1) format Null; -- monitor_sleep_time_ms
select value from system.metrics where metric = 'DistributedFilesToInsert';
system start distributed sends dist_01460;
select sleep(1) format Null; -- monitor_sleep_time_ms
select value from system.metrics where metric = 'DistributedFilesToInsert';

select 'FLUSH DISTRIBUTED';
system stop distributed sends dist_01460;
insert into dist_01460 select * from system.one;
select sleep(1) format Null; -- monitor_sleep_time_ms
select value from system.metrics where metric = 'DistributedFilesToInsert';
system flush distributed dist_01460;
select value from system.metrics where metric = 'DistributedFilesToInsert';

select 'DROP STREAM';
system stop distributed sends dist_01460;
insert into dist_01460 select * from system.one;
select sleep(1) format Null; -- monitor_sleep_time_ms
select value from system.metrics where metric = 'DistributedFilesToInsert';
drop stream dist_01460;
select value from system.metrics where metric = 'DistributedFilesToInsert';

drop stream data_01460;
