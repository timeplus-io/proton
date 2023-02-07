-- Tags: no-parallel, no-fasttest

insert into function file(02422_data.msgpack) select to_uuid('f4cdd80d-5d15-4bdc-9527-adcca635ec1f') as uuid settings output_format_msgpack_uuid_representation='ext';
select * from file(02422_data.msgpack, auto, 'x int32'); -- {serverError ILLEGAL_COLUMN}
