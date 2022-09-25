-- Tags: zookeeper, no-replicated-database, no-parallel
SET query_mode = 'table';
drop stream if exists x;

create stream x(i int, index mm RAND() type minmax granularity 1, projection p (select MAX(i))) engine ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r') order by i;

alter stream x add index nn RAND() type minmax granularity 1, add projection p2 (select MIN(i));

show create x;

select value from system.zookeeper WHERE name = 'metadata' and path = '/clickhouse/tables/'||currentDatabase()||'/x';

drop stream x;
