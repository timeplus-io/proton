-- force data path with the user/pass in it
set use_compact_format_in_distributed_parts_names=0;
-- use async send even for localhost
set prefer_localhost_replica=0;

SET query_mode = 'table';
drop stream if exists dist_01555;
drop stream if exists data_01555;
create stream data_01555 (key int) Engine=Null();

--
-- masked
--
SELECT 'masked';
create stream dist_01555 (key int) Engine=Distributed(test_cluster_with_incorrect_pw, currentDatabase(), data_01555, key);

insert into dist_01555 values (1)(2);
-- since test_cluster_with_incorrect_pw contains incorrect password ignore error
system flush distributed dist_01555; -- { serverError 516; }
select length(splitByChar('*', data_path)),  replace_regexp_one(data_path, '^.*/([^/]*)/' , '\\1') from system.distribution_queue where database = currentDatabase() and table = 'dist_01555' format CSV;

drop stream dist_01555;

--
-- no masking
--
SELECT 'no masking';
create stream dist_01555 (key int) Engine=Distributed(test_shard_localhost, currentDatabase(), data_01555, key);

insert into dist_01555 values (1)(2);
-- since test_cluster_with_incorrect_pw contains incorrect password ignore error
system flush distributed dist_01555;
select length(splitByChar('*', data_path)),  replace_regexp_one(data_path, '^.*/([^/]*)/' , '\\1') from system.distribution_queue where database = currentDatabase() and table = 'dist_01555' format CSV;

-- cleanup
drop stream dist_01555;
drop stream data_01555;
