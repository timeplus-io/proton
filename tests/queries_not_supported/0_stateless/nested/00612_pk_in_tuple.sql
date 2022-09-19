SET query_mode = 'table';
drop stream if exists tab_00612;
create stream tab_00612 (key uint64, arr array(uint64)) Engine = MergeTree order by key;
insert into tab_00612 values (1, [1]);
insert into tab_00612 values (2, [2]);
select 'all';
select * from tab_00612 order by key;
select 'key, array_join(arr) in (1, 1)';
select key, array_join(arr) as val from tab_00612 where (key, val) in (1, 1);
select 'key, array_join(arr) in ((1, 1), (2, 2))';
select key, array_join(arr) as val from tab_00612 where (key, val) in ((1, 1), (2, 2)) order by key;
select '(key, left array join arr) in (1, 1)';
select key from tab_00612 left array join arr as val where (key, val) in (1, 1);
select '(key, left array join arr) in ((1, 1), (2, 2))';
select key from tab_00612 left array join arr as val where (key, val) in ((1, 1), (2, 2)) order by key;

drop stream if exists tab_00612;
create stream tab_00612 (key uint64, n nested(x uint64)) Engine = MergeTree order by key;
insert into tab_00612 values (1, [1]);
insert into tab_00612 values (2, [2]);
select 'all';
select * from tab_00612 order by key;
select 'key, array_join(n.x) in (1, 1)';
select key, array_join(n.x) as val from tab_00612 where (key, val) in (1, 1);
select 'key, array_join(n.x) in ((1, 1), (2, 2))';
select key, array_join(n.x) as val from tab_00612 where (key, val) in ((1, 1), (2, 2)) order by key;
select '(key, left array join n.x) in (1, 1)';
select key from tab_00612 left array join n.x as val where (key, val) in (1, 1);
select '(key, left array join n.x) in ((1, 1), (2, 2))';
select key from tab_00612 left array join n.x as val where (key, val) in ((1, 1), (2, 2)) order by key;
select 'max(key) from tab_00612 where (key, left array join n.x) in (1, 1)';
select max(key) from tab_00612 left array join `n.x` as val where (key, val) in ((1, 1));
select max(key) from tab_00612 left array join n as val where (key, val.x) in (1, 1);
select 'max(key) from tab_00612 where (key, left array join n.x) in ((1, 1), (2, 2))';
select max(key) from tab_00612 left array join `n.x` as val where (key, val) in ((1, 1), (2, 2));
select max(key) from tab_00612 left array join n as val where (key, val.x) in ((1, 1), (2, 2));
select 'max(key) from tab_00612 any left join (select key, array_join(n.x) as val from tab_00612) using key where (key, val) in (1, 1)';
select max(key) from tab_00612 any left join (select key, array_join(n.x) as val from tab_00612) js2 using key where (key, val) in (1, 1);
select 'max(key) from tab_00612 any left join (select key, array_join(n.x) as val from tab_00612) using key where (key, val) in ((1, 1), (2, 2))';
select max(key) from tab_00612 any left join (select key, array_join(n.x) as val from tab_00612) js2 using key where (key, val) in ((1, 1), (2, 2));

drop  table if exists tab_00612;
create stream tab_00612 (key1 int32, id1  Int64, c1 Int64) ENGINE = MergeTree  PARTITION BY id1 ORDER BY (key1) ;
insert into tab_00612 values ( -1, 1, 0 );
SELECT count(*) FROM  tab_00612 PREWHERE id1 IN (1);

SELECT count() FROM tab_00612 WHERE (key1, id1) IN (-1, 1) AND (key1, 1) IN (-1, 1) SETTINGS force_primary_key = 1;

drop stream tab_00612;
