
drop stream if exists mt;
drop stream if exists m;

create stream mt (p int, n int) engine=MergeTree order by tuple() partition by p;
create stream m (n int) engine=Memory;
insert into mt values (1, 1), (2, 1);
insert into mt values (1, 2), (2, 2);
select *, _part from mt order by _part;

alter stream  mt update n = n + (n not in m) in partition id '1' where 1 settings mutations_sync=1;
drop stream m;
optimize stream mt final;

select mutation_id, command, parts_to_do_names, parts_to_do, is_done from system.mutations where database=currentDatabase();
select * from mt order by p, n;

drop stream mt;
