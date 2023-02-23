drop stream if exists cc sync;
create stream cc (a uint64, b string) ENGINE = MergeTree order by (a, b) SETTINGS compress_marks = true;
insert into cc  values (2, 'World');
alter stream cc detach part 'all_1_1_0';
alter stream cc attach part 'all_1_1_0';
select * from cc;
