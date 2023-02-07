-- Tags: no-parallel, no-fasttest

insert into function file(data_02313.avro) select tuple(number, 'string')::Tuple(a uint32, b string) as t from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02313.avro);
select * from file(data_02313.avro);

insert into function file(data_02313.avro) select tuple(number, tuple(number + 1, number + 2), range(number))::Tuple(a uint32, b Tuple(c uint32, d uint32), e array(uint32)) as t from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02313.avro);
select * from file(data_02313.avro);

insert into function file(data_02313.avro, auto, 'a Nested(b uint32, c uint32)') select [number, number + 1], [number + 2, number + 3] from  numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02313.avro);
select * from file(data_02313.avro);

insert into function file(data_02313.avro, auto, 'a Nested(b Nested(c uint32, d uint32))') select [[(number, number + 1), (number + 2, number + 3)]] from  numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02313.avro);
select * from file(data_02313.avro);

insert into function file(data_02313.avro) select map(concat('key_', to_string(number)), number) as m from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02313.avro);
select * from file(data_02313.avro);

insert into function file(data_02313.avro) select map(concat('key_', to_string(number)), tuple(number, range(number))) as m from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02313.avro);
select * from file(data_02313.avro);

