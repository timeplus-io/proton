-- Tags: zookeeper

SET allow_experimental_analyzer = 1;

create stream rmt1 (d datetime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '1') order by n partition by toYYYYMMDD(d);
create stream rmt2 (d datetime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '2') order by n partition by toYYYYMMDD(d);

system stop replicated sends rmt1;
insert into rmt1 values (now(), arrayJoin([1, 2])); -- { clientError 36 }
insert into rmt1(n) select * from system.numbers limit arrayJoin([1, 2]); -- { serverError 440 }
insert into rmt1 values (now(), rand());
drop stream rmt1;

system sync replica rmt2;
drop stream rmt2;


create stream rmt1 (d datetime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '1') order by n partition by tuple();
create stream rmt2 (d datetime, n int) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '2') order by n partition by tuple();

system stop replicated sends rmt1;
insert into rmt1 values (now(), rand());
drop stream rmt1;

system sync replica rmt2;
drop stream rmt2;


create stream rmt1 (n UInt8, m Int32, d Date, t datetime) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '1') order by n partition by (n, m, d, t);
create stream rmt2 (n UInt8, m Int32, d Date, t datetime) engine=ReplicatedMergeTree('/test/01165/{database}/rmt', '2') order by n partition by (n, m, d, t);

system stop replicated sends rmt1;
insert into rmt1 values (rand(), rand(), now(), now());
insert into rmt1 values (rand(), rand(), now(), now());
insert into rmt1 values (rand(), rand(), now(), now());
drop stream rmt1;

system sync replica rmt2;
drop stream rmt2;