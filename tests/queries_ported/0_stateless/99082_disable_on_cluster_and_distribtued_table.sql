-- Test cases for disabled ON CLUSTER syntax in Proton
-- disable distributed table


CREATE stream sharded_table (dummy uint8) ENGINE = Distributed('test_cluster_two_shards', 'system', 'one'); -- {serverError UNSUPPORTED}


-- All these should result in SYNTAX_ERROR since we've disabled ON CLUSTER parsing

-- Basic CREATE STREAM with ON CLUSTER
create stream v(id int) on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE STREAM with IF NOT EXISTS and ON CLUSTER
create stream if not exists v(id int) on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE STREAM with database.table and ON CLUSTER
create stream db.v(id int) on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE STREAM with ENGINE and ON CLUSTER
create stream v(id int) engine=MergeTree() on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE STREAM with ORDER BY and ON CLUSTER
create stream v(id int) engine=MergeTree() order by id on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP STREAM with ON CLUSTER
drop stream v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP STREAM IF EXISTS with ON CLUSTER
drop stream if exists v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP DATABASE with ON CLUSTER
drop database db on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP DATABASE IF EXISTS with ON CLUSTER
drop database if exists db on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- RENAME STREAM with ON CLUSTER
rename stream v to v2 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- RENAME STREAM IF EXISTS with ON CLUSTER
rename stream if exists v to v2 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- RENAME STREAM with database.table and ON CLUSTER
rename stream db.v to db.v2 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- OPTIMIZE STREAM with ON CLUSTER
optimize stream v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- OPTIMIZE STREAM FINAL with ON CLUSTER
optimize stream v final on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- OPTIMIZE STREAM PARTITION with ON CLUSTER
optimize stream v partition '2023-01-01' on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DELETE with ON CLUSTER
delete from v where id = 1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DELETE with database.table and ON CLUSTER
delete from db.v where id = 1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- SYSTEM queries with ON CLUSTER
system reload dictionary dict on cluster cluster_1; -- {clientError SYNTAX_ERROR }

system reload model model_name on cluster cluster_1; -- {clientError SYNTAX_ERROR }

system reload function func_name on cluster cluster_1; -- {clientError SYNTAX_ERROR }

system drop replica 'replica_name' from database db on cluster cluster_1; -- {clientError SYNTAX_ERROR }

system restart replica db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

system sync replica db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

system wait loading parts db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- KILL QUERY with ON CLUSTER
kill query where query_id = '123' on cluster cluster_1; -- {clientError SYNTAX_ERROR }

kill mutation where mutation_id = '456' on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- Access Control queries with ON CLUSTER
-- CREATE USER with ON CLUSTER
create user user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

create user user1 identified by 'password' on cluster cluster_1; -- {clientError SYNTAX_ERROR }

create user user1 identified with plaintext_password by 'password' on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- ALTER USER with ON CLUSTER
alter user user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

alter user user1 identified by 'newpassword' on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP USER with ON CLUSTER
drop user user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop user if exists user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE ROLE with ON CLUSTER
create role role1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

create role if not exists role1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- ALTER ROLE with ON CLUSTER
alter role role1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP ROLE with ON CLUSTER
drop role role1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop role if exists role1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE QUOTA with ON CLUSTER
create quota quota1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

create quota quota1 for interval 1 hour max queries 100 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- ALTER QUOTA with ON CLUSTER
alter quota quota1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP QUOTA with ON CLUSTER
drop quota quota1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop quota if exists quota1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE SETTINGS PROFILE with ON CLUSTER
create settings profile profile1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

create settings profile profile1 settings max_memory_usage = 1000000 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- ALTER SETTINGS PROFILE with ON CLUSTER
alter settings profile profile1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP SETTINGS PROFILE with ON CLUSTER
drop settings profile profile1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop settings profile if exists profile1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE ROW POLICY with ON CLUSTER
create row policy policy1 on db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

create row policy policy1 on db.v for select using id > 0 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- ALTER ROW POLICY with ON CLUSTER
alter row policy policy1 on db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP ROW POLICY with ON CLUSTER
drop row policy policy1 on db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop row policy if exists policy1 on db.v on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- GRANT with ON CLUSTER
grant select on db.v to user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

grant all on db.* to user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

grant role1 to user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- REVOKE with ON CLUSTER
revoke select on db.v from user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

revoke all on db.* from user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

revoke role1 from user1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE FUNCTION with ON CLUSTER
create function func1(x int) returns int as x * 2 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP FUNCTION with ON CLUSTER
drop function func1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop function if exists func1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE FORMAT SCHEMA with ON CLUSTER
create format schema schema1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP FORMAT SCHEMA with ON CLUSTER
drop format schema schema1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop format schema if exists schema1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE NAMED COLLECTION with ON CLUSTER
create named collection collection1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- ALTER NAMED COLLECTION with ON CLUSTER
alter named collection collection1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP NAMED COLLECTION with ON CLUSTER
drop named collection collection1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop named collection if exists collection1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE DISK with ON CLUSTER
create disk disk1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP DISK with ON CLUSTER
drop disk disk1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop disk if exists disk1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- CREATE STORAGE POLICY with ON CLUSTER
create storage policy policy1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- DROP STORAGE POLICY with ON CLUSTER
drop storage policy policy1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

drop storage policy if exists policy1 on cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- Complex combinations
-- Multiple ON CLUSTER in different positions
create stream v(id int) engine=MergeTree() order by id on cluster cluster_1 partition by toYYYYMM(date) on cluster cluster_2; -- {clientError SYNTAX_ERROR }

-- ON CLUSTER with complex expressions
create stream v(id int) on cluster 'cluster_with_underscores'; -- {clientError SYNTAX_ERROR }

create stream v(id int) on cluster "cluster_with_quotes"; -- {clientError SYNTAX_ERROR }

-- ON CLUSTER with special characters in cluster name
create stream v(id int) on cluster cluster-1; -- {clientError SYNTAX_ERROR }

create stream v(id int) on cluster cluster_1_test; -- {clientError SYNTAX_ERROR }

-- ON CLUSTER with numbers
create stream v(id int) on cluster cluster123; -- {clientError SYNTAX_ERROR }

-- Multiple statements with ON CLUSTER
create stream v1(id int) on cluster cluster_1; create stream v2(id int) on cluster cluster_2; -- {clientError SYNTAX_ERROR }

-- ON CLUSTER with comments
create stream v(id int) on cluster cluster_1; -- comment -- {clientError SYNTAX_ERROR }

-- ON CLUSTER with semicolon
create stream v(id int) on cluster cluster_1;; -- {clientError SYNTAX_ERROR }

-- Test that ON CLUSTER is completely disabled
-- These should all fail with SYNTAX_ERROR
create stream v(id int) on cluster; -- {clientError SYNTAX_ERROR }

create stream v(id int) on; -- {clientError SYNTAX_ERROR }

create stream v(id int) cluster cluster_1; -- {clientError SYNTAX_ERROR }

-- Test edge cases
create stream v(id int) on cluster ''; -- {clientError SYNTAX_ERROR }

create stream v(id int) on cluster '   '; -- {clientError SYNTAX_ERROR }

-- Test that the parser doesn't get confused by similar keywords
create stream v(id int) on; -- {clientError SYNTAX_ERROR }

create stream v(id int) cluster; -- {clientError SYNTAX_ERROR }
