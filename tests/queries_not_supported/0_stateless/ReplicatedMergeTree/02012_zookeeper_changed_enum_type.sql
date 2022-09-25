-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards
SET query_mode = 'table';
create stream enum_alter_issue (a enum8('one' = 1, 'two' = 2), b int)
engine = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02012/enum_alter_issue', 'r1')
ORDER BY a;

insert into enum_alter_issue values ('one', 1), ('two', 2);
alter stream enum_alter_issue modify column a enum8('one' = 1, 'two' = 2, 'three' = 3);
insert into enum_alter_issue values ('one', 3), ('two', 4);

alter stream enum_alter_issue detach partition id 'all';
alter stream enum_alter_issue attach partition id 'all';
select * from enum_alter_issue order by b;

drop stream enum_alter_issue;
