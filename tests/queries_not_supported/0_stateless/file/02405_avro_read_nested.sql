-- Tags: no-fasttest, no-parallel

set flatten_nested = 1;

insert into function file(02405_data.avro) select [(1, 'aa'), (2, 'bb')]::Nested(x uint32, y string) as nested settings engine_file_truncate_on_insert=1;
select * from file(02405_data.avro, auto, 'nested Nested(x uint32, y string)');

insert into function file(02405_data.avro) select [(1, (2, ['aa', 'bb']), [(3, 'cc'), (4, 'dd')]), (5, (6, ['ee', 'ff']), [(7, 'gg'), (8, 'hh')])]::Nested(x uint32, y tuple(y1 uint32, y2 array(string)), z Nested(z1 uint32, z2 string)) as nested settings engine_file_truncate_on_insert=1;
select * from file(02405_data.avro, auto, 'nested Nested(x uint32, y tuple(y1 uint32, y2 array(string)), z Nested(z1 uint32, z2 string))');
