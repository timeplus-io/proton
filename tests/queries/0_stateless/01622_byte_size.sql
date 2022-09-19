--
-- byteSize
--
select '';
select '# byteSize';

set allow_experimental_bigint_types = 1;

-- numbers #0 --
select '';
select 'byteSize for numbers #0';

SET query_mode = 'table';
drop stream if exists test_byte_size_number0;
create stream test_byte_size_number0
(
    key int32,
    u8 uint8,
    u16 uint16,
    u32 uint32,
    u64 uint64,
    u256 UInt256,
    i8 int8,
    i16 Int16,
    i32 int32,
    i64 int64,
    i128 Int128,
    i256 Int256,
    f32 Float32,
    f64 float64
) engine MergeTree order by key;

insert into test_byte_size_number0 values(1, 8, 16, 32, 64, 256, -8, -16, -32, -64, -128, -256, 32.32, 64.64);
insert into test_byte_size_number0 values(2, 8, 16, 32, 64, 256, -8, -16, -32, -64, -128, -256, 32.32, 64.64);

select key, to_type_name(u8), byteSize(u8), to_type_name(u16), byteSize(u16), to_type_name(u32), byteSize(u32), to_type_name(u64), byteSize(u64), to_type_name(u256), byteSize(u256) from test_byte_size_number0 order by key;
select key, to_type_name(i8), byteSize(i8), to_type_name(i16), byteSize(i16), to_type_name(i32), byteSize(i32), to_type_name(i64), byteSize(i64), to_type_name(i128), byteSize(i128), to_type_name(u256), byteSize(u256) from test_byte_size_number0 order by key;
select key, to_type_name(f32), byteSize(f32), to_type_name(f64), byteSize(f64) from test_byte_size_number0 order by key;

drop stream if exists test_byte_size_number0;


-- numbers #1 --
select '';
select 'byteSize for numbers #1';
drop stream if exists test_byte_size_number1;
create stream test_byte_size_number1
(
    key int32,
    date date,
    dt DateTime,
    dt64 DateTime64(3),
    en8 Enum8('a'=1, 'b'=2, 'c'=3, 'd'=4),
    en16 Enum16('c'=100, 'l'=101, 'i'=102, 'ck'=103, 'h'=104, 'o'=105, 'u'=106, 's'=107, 'e'=108),
    dec32 Decimal32(4),
    dec64 Decimal64(8),
    dec128 Decimal128(16),
    dec256 Decimal256(16),
    uuid UUID
) engine MergeTree order by key;

insert into test_byte_size_number1 values(1, '2020-01-01', '2020-01-01 01:02:03', '2020-02-02 01:02:03', 'a', 'ck', 32.32, 64.64, 128.128, 256.256, generateUUIDv4());
insert into test_byte_size_number1 values(2, '2020-01-01', '2020-01-01 01:02:03', '2020-02-02 01:02:03', 'a', 'ck', 32.32, 64.64, 128.128, 256.256, generateUUIDv4());

select key, byteSize(*), to_type_name(date), byteSize(date), to_type_name(dt), byteSize(dt), to_type_name(dt64), byteSize(dt64), to_type_name(uuid), byteSize(uuid) from test_byte_size_number1 order by key;

drop stream if exists test_byte_size_number1;


-- constant numbers --
select '';
select 'byteSize for constants';
select 0x1, byteSize(0x1), 0x100, byteSize(0x100), 0x10000, byteSize(0x10000), 0x100000000, byteSize(0x100000000), 0.5, byteSize(0.5), 1e-10, byteSize(1e-10);
select to_date('2020-01-01'), byteSize(to_date('2020-01-01')), to_datetime('2020-01-01 01:02:03'), byteSize(to_datetime('2020-01-01 01:02:03')), toDateTime64('2020-01-01 01:02:03',3), byteSize(toDateTime64('2020-01-01 01:02:03',3));
select to_type_name(generateUUIDv4()), byteSize(generateUUIDv4());


-- strings --
select '';
select 'byteSize for strings';
drop stream if exists test_byte_size_string;
create stream test_byte_size_string
(
    key int32,
    str1 string,
    str2 string,
    fstr1 FixedString(8),
    fstr2 FixedString(8)
) engine MergeTree order by key;

insert into test_byte_size_string values(1, '', 'a', '', 'abcde');
insert into test_byte_size_string values(2, 'abced', '', 'abcde', '');

select key, byteSize(*), str1, byteSize(str1), str2, byteSize(str2), fstr1, byteSize(fstr1), fstr2, byteSize(fstr2) from test_byte_size_string order by key;
select 'constants: ', '', byteSize(''), 'a', byteSize('a'), 'abcde', byteSize('abcde');

drop stream if exists test_byte_size_string;


-- simple arrays --
drop stream if exists test_byte_size_array;
create stream test_byte_size_array
(
    key int32,
    uints8 array(uint8),
    ints8 array(int8),
    ints32 array(int32),
    floats32 array(Float32),
    decs32 array(Decimal32(4)),
    dates array(date),
    uuids array(UUID)
) engine MergeTree order by key;

insert into test_byte_size_array values(1, [], [], [], [], [], [], []);
insert into test_byte_size_array values(2, [1], [-1], [256], [1.1], [1.1], ['2020-01-01'], ['61f0c404-5cb3-11e7-907b-a6006ad3dba0']);
insert into test_byte_size_array values(3, [1,1], [-1,-1], [256,256], [1.1,1.1], [1.1,1.1], ['2020-01-01','2020-01-01'], ['61f0c404-5cb3-11e7-907b-a6006ad3dba0','61f0c404-5cb3-11e7-907b-a6006ad3dba0']);
insert into test_byte_size_array values(4, [1,1,1], [-1,-1,-1], [256,256,256], [1.1,1.1,1.1], [1.1,1.1,1.1], ['2020-01-01','2020-01-01','2020-01-01'], ['61f0c404-5cb3-11e7-907b-a6006ad3dba0','61f0c404-5cb3-11e7-907b-a6006ad3dba0','61f0c404-5cb3-11e7-907b-a6006ad3dba0']);

select '';
select 'byteSize for simple array';
select key, byteSize(*), uints8, byteSize(uints8), ints8, byteSize(ints8), ints32, byteSize(ints32), floats32, byteSize(floats32), decs32, byteSize(decs32), dates, byteSize(dates), uuids, byteSize(uuids) from test_byte_size_array order by key;

select 'constants:', [], byteSize([]), [1,1], byteSize([1,1]), [-1,-1], byteSize([-1,-1]), to_type_name([256,256]), byteSize([256,256]), to_type_name([1.1,1.1]), byteSize([1.1,1.1]);
select 'constants:', [to_decimal32(1.1,4),to_decimal32(1.1,4)], byteSize([to_decimal32(1.1,4),to_decimal32(1.1,4)]), [to_date('2020-01-01'),to_date('2020-01-01')], byteSize([to_date('2020-01-01'),to_date('2020-01-01')]);
select 'constants:', [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'),toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], byteSize([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'),toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]);

drop stream if exists test_byte_size_array;


-- complex arrays --
drop stream if exists test_byte_size_complex_array;
create stream test_byte_size_complex_array
(
    key int32,
    ints array(int32),
    int_ints array(array(int32)),
    strs array(string),
    str_strs array(array(string))
) engine MergeTree order by key;

insert into test_byte_size_complex_array values(1, [], [[]], [], [[]]);
insert into test_byte_size_complex_array values(2, [1,2], [[], [1,2]], [''], [[], ['']]);
insert into test_byte_size_complex_array values(3, [0,256], [[], [1,2], [0,256]], ['','a'], [[], [''], ['','a']]);
insert into test_byte_size_complex_array values(4, [256,65536], [[], [1,2], [0,256], [256,65536]], ['','a','abced'], [[], [''], ['','a'], ['','a','abced']]);

select '';
select 'byteSize for int array of arrays';
select key, byteSize(*), ints, byteSize(ints), int_ints, byteSize(int_ints) from test_byte_size_complex_array order by key;
select 'constants:', [[], [1,2], [0,0x10000]],to_type_name([[], [1,2], [0,0x10000]]), byteSize([[], [1,2], [0,0x10000]]);

select '';
select 'byteSize for string array of arrays';
-- select key, byteSize(*), strs, byteSize(strs), str_strs, byteSize(str_strs) from test_byte_size_complex_array order by key;
select key, byteSize(*), strs, byteSize(strs), str_strs, byteSize(str_strs) from test_byte_size_complex_array order by key;
select 'constants:', [[], [''], ['','a']], byteSize([[], [''], ['','a']]);

drop stream if exists test_byte_size_complex_array;


-- others --
drop stream if exists test_byte_size_other;
create stream test_byte_size_other
(
    key int32,
    opt_int32 Nullable(int32),
    opt_str Nullable(string),
    tuple tuple(int32, Nullable(string)),
    strings LowCardinality(string)
) engine MergeTree order by key;

insert into test_byte_size_other values(1, NULL, NULL, tuple(1, NULL), '');
insert into test_byte_size_other values(2, 1, 'a', tuple(1, 'a'), 'a');
insert into test_byte_size_other values(3, 256, 'abcde', tuple(256, 'abcde'), 'abcde');

select '';
select 'byteSize for others: Nullable, tuple, LowCardinality';
select key, byteSize(*), opt_int32, byteSize(opt_int32), opt_str, byteSize(opt_str), tuple, byteSize(tuple), strings, byteSize(strings) from test_byte_size_other order by key;
select 'constants:', NULL, byteSize(NULL), tuple(0x10000, NULL), byteSize(tuple(0x10000, NULL)), tuple(0x10000, toNullable('a')), byteSize(tuple(0x10000, toNullable('a')));
select 'constants:', toLowCardinality('abced'),to_type_name(toLowCardinality('abced')), byteSize(toLowCardinality('abced'));

drop stream if exists test_byte_size_other;


-- more complex fields --
drop stream if exists test_byte_size_more_complex;
create stream test_byte_size_more_complex
(
    key int32,
    complex1 array(tuple(Nullable(FixedString(4)), array(tuple(Nullable(string), string))))
) engine MergeTree order by key;

insert into test_byte_size_more_complex values(1, []);
insert into test_byte_size_more_complex values(2, [tuple(NULL, [])]);
insert into test_byte_size_more_complex values(3, [tuple('a', [])]);
insert into test_byte_size_more_complex values(4, [tuple('a', [tuple(NULL, 'a')])]);
insert into test_byte_size_more_complex values(5, [tuple('a', [tuple(NULL, 'a'), tuple(NULL, 'a')])]);
insert into test_byte_size_more_complex values(6, [tuple(NULL, []), tuple('a', []), tuple('a', [tuple(NULL, 'a')]), tuple('a', [tuple(NULL, 'a'), tuple(NULL, 'a')])]);

select '';
select 'byteSize for complex fields';
select key, byteSize(*), complex1, byteSize(complex1) from test_byte_size_more_complex order by key;
select 'constants:', tuple(NULL, []), byteSize(tuple(NULL, [])), tuple(toNullable(to_fixed_string('a',4)), []), byteSize(tuple(toNullable(to_fixed_string('a',4)), [])), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a')]), byteSize(tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a')])), tuple(to_fixed_string('a',4), [tuple(NULL, 'a'), tuple(NULL, 'a')]), byteSize(tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')]));
select 'constants:', [tuple(NULL, []), tuple(toNullable(to_fixed_string('a',4)), []), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a')]), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')])];
select 'constants:', to_type_name([tuple(NULL, []), tuple(toNullable(to_fixed_string('a',4)), []), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a')]), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')])]);
select 'constants:', byteSize([tuple(NULL, []), tuple(toNullable(to_fixed_string('a',4)), []), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a')]), tuple(toNullable(to_fixed_string('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')])]);

drop stream if exists test_byte_size_more_complex;
