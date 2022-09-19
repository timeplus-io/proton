-- Tags: not_supported, blocked_by_SummingMergeTrees

SET query_mode = 'table';

drop stream if exists nested_map;

create stream nested_map (d default today(), k uint64, payload default rand(), SomeMap nested(ID uint32, Num Int64)) engine=SummingMergeTree(d, k, 8192);

insert into nested_map (k, `SomeMap.ID`, `SomeMap.Num`) values (0,[1],[100]),(1,[1],[100]),(2,[1],[100]),(3,[1,2],[100,150]);
insert into nested_map (k, `SomeMap.ID`, `SomeMap.Num`) values (0,[2],[150]),(1,[1],[150]),(2,[1,2],[150,150]),(3,[1],[-100]);
optimize table nested_map;
select `SomeMap.ID`, `SomeMap.Num` from nested_map;

drop stream nested_map;

create stream nested_map (d default today(), k uint64, payload default rand(), SomeMap nested(ID string, Num Int64)) engine=SummingMergeTree(d, k, 8192);

insert into nested_map (k, `SomeMap.ID`, `SomeMap.Num`) values (0,['1'],[100]),(1,['1'],[100]),(2,['1'],[100]),(3,['1','2'],[100,150]);
insert into nested_map (k, `SomeMap.ID`, `SomeMap.Num`) values (0,['2'],[150]),(1,['1'],[150]),(2,['1','2'],[150,150]),(3,['1'],[-100]);
optimize table nested_map;
select `SomeMap.ID`, `SomeMap.Num` from nested_map;

drop stream nested_map;

drop stream if exists nested_map_explicit;

create stream nested_map_explicit (d default today(), k uint64, SomeIntExcluded uint32, SomeMap nested(ID uint32, Num Int64)) engine=SummingMergeTree(d, k, 8192, (SomeMap));

insert into nested_map_explicit (k, `SomeIntExcluded`, `SomeMap.ID`, `SomeMap.Num`) values (0, 20, [1],[100]),(1, 20, [1],[100]),(2, 20, [1],[100]),(3, 20, [1,2],[100,150]);
insert into nested_map_explicit (k, `SomeIntExcluded`, `SomeMap.ID`, `SomeMap.Num`) values (0, 20, [2],[150]),(1, 20, [1],[150]),(2, 20, [1,2],[150,150]),(3, 20, [1],[-100]);
optimize table nested_map_explicit;
select `SomeIntExcluded`, `SomeMap.ID`, `SomeMap.Num` from nested_map_explicit;

drop stream nested_map_explicit;
