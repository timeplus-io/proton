-- Tags: no-parallel
select * from values('x uint8 NOT NULL', 1);
select * from values('x uint8 NULL', NULL);
insert into function file(data_02323.tsv) select number % 2 ? number : NULL from numbers(3) settings engine_file_truncate_on_insert=1;
select * from file(data_02323.tsv, auto, 'x uint32 NOT NULL');
select * from file(data_02323.tsv, auto, 'x uint32 NULL');
select * from generateRandom('x uint64 NULL', 7, 3) limit 2;
