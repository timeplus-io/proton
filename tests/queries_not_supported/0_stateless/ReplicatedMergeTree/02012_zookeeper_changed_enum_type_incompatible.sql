-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards
SET query_mode = 'table';
drop stream if exists enum_alter_issue;
create stream enum_alter_issue (a Enum16('one' = 1, 'two' = 2), b int)
engine = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02012/enum_alter_issue', 'r2')
ORDER BY b;

insert into enum_alter_issue values ('one', 1), ('two', 1);
alter stream enum_alter_issue detach partition id 'all';
alter stream enum_alter_issue modify column a enum8('one' = 1, 'two' = 2, 'three' = 3);
insert into enum_alter_issue values ('one', 1), ('two', 1);

alter stream enum_alter_issue attach partition id 'all'; -- {serverError TYPE_MISMATCH}
drop stream enum_alter_issue;
