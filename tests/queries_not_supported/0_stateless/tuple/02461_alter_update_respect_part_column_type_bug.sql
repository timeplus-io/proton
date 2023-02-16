drop stream if exists src;
create stream src( A int64, B string, C string) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values(1, 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column B nullable(string);
alter stream src attach partition tuple();

alter stream src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop stream if exists src;
create stream src( A string, B string, C string) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column A low_cardinality(string);
alter stream src attach partition tuple();

alter stream src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop stream if exists src;
create stream src( A string, B string, C string) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column A low_cardinality(string);
alter stream src attach partition tuple();

alter stream src modify column C low_cardinality(string);
select * from src;

drop stream if exists src;
create stream src( A string, B string, C string) Engine=MergeTree order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column B nullable(string);
alter stream src attach partition tuple();

alter stream src rename column B to D;
select * from src;

select '-----';

drop stream if exists src;
create stream src( A int64, B string, C string) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src1', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values(1, 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column B nullable(string);
alter stream src attach partition tuple();

alter stream src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop stream if exists src;
create stream src( A string, B string, C string) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src2', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column A low_cardinality(string);
alter stream src attach partition tuple();

alter stream src update C = 'test1' where 1 settings mutations_sync=2;
select * from src;


drop stream if exists src;
create stream src( A string, B string, C string) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src3', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column A low_cardinality(string);
alter stream src attach partition tuple();

alter stream src modify column C low_cardinality(string);
select * from src;

drop stream if exists src;
create stream src( A string, B string, C string) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src4', '1') order by A SETTINGS min_bytes_for_wide_part=0;
insert into src values('one', 'one', 'test');

alter stream src detach partition tuple();
alter stream src modify column B nullable(string);
alter stream src attach partition tuple();

alter stream src rename column B to D;
select * from src;

