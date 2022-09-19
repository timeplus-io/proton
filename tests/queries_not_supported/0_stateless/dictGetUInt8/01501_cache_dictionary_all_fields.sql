-- Tags: no-parallel

drop database if exists db_01501;
create database db_01501;

SET query_mode = 'table';
create stream db_01501.table_cache_dict(
KeyField uint64,
UInt8_ uint8,
UInt16_ UInt16,
UInt32_ uint32,
UInt64_ uint64,
Int8_ int8,
Int16_ Int16,
Int32_ int32,
Int64_ Int64,
UUID_ UUID,
Date_ date,
DateTime_ DateTime,
String_ string,
Float32_ Float32,
Float64_ float64,
Decimal32_ Decimal32(5),
Decimal64_ Decimal64(15),
Decimal128_ Decimal128(35),
ParentKeyField uint64)
ENGINE = MergeTree() ORDER BY KeyField;


CREATE DICTIONARY IF NOT EXISTS db_01501.cache_dict (
	KeyField uint64 DEFAULT 9999999,
	UInt8_ uint8 DEFAULT 55,
	UInt16_ UInt16 DEFAULT 65535,
	UInt32_ uint32 DEFAULT 4294967295,
	UInt64_ uint64 DEFAULT 18446744073709551615,
	Int8_ int8 DEFAULT -128,
	Int16_ Int16 DEFAULT -32768,
	Int32_ int32 DEFAULT -2147483648,
	Int64_ Int64 DEFAULT -9223372036854775808,
	UUID_ UUID DEFAULT '550e8400-0000-0000-0000-000000000000',
	Date_ date DEFAULT '2018-12-30',
	DateTime_ DateTime DEFAULT '2018-12-30 00:00:00',
	String_ string DEFAULT 'hi',
	Float32_ Float32 DEFAULT 111.11,
	Float64_ float64 DEFAULT 222.11,
	Decimal32_ Decimal32(5) DEFAULT 333.11,
	Decimal64_ Decimal64(15) DEFAULT 444.11,
	Decimal128_ Decimal128(35) DEFAULT 555.11,
	ParentKeyField uint64 DEFAULT 444)
PRIMARY KEY KeyField
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_cache_dict' DB 'db_01501'))
LIFETIME(5) LAYOUT(CACHE(SIZE_IN_CELLS 20));


INSERT INTO db_01501.table_cache_dict VALUES (1, 2, 3, 4, 5, -1, -2, -3, -4, '550e8400-e29b-41d4-a716-446655440003', '1973-06-28', '1985-02-28 23:43:25', 'clickhouse', 22.543, 3332154213.4, to_decimal32('1e-5', 5), to_decimal64('1e-15', 15), toDecimal128('1e-35', 35), 0);
INSERT INTO db_01501.table_cache_dict VALUES (2, 22, 33, 44, 55, -11, -22, -33, -44, 'cb307805-44f0-49e7-9ae9-9954c543be46', '1978-06-28', '1986-02-28 23:42:25', 'hello', 21.543, 3111154213.9, to_decimal32('2e-5', 5), to_decimal64('2e-15', 15), toDecimal128('2e-35', 35), 1);
INSERT INTO db_01501.table_cache_dict VALUES (3, 222, 333, 444, 555, -111, -222, -333, -444, 'de7f7ec3-f851-4f8c-afe5-c977cb8cea8d', '1982-06-28', '1999-02-28 23:42:25', 'dbms', 13.334, 3222187213.1, to_decimal32('3e-5', 5), to_decimal64('3e-15', 15), toDecimal128('3e-35', 35), 1);
INSERT INTO db_01501.table_cache_dict VALUES (4, 2222, 3333, 4444, 5555, -1111, -2222, -3333, -4444, '4bd3829f-0669-43b7-b884-a8e034a68224', '1987-06-28', '2000-02-28 23:42:25', 'MergeTree', 52.001, 3237554213.5, to_decimal32('4e-5', 5), to_decimal64('4e-15', 15), toDecimal128('4e-35', 35), 1);
INSERT INTO db_01501.table_cache_dict VALUES (5, 22222, 33333, 44444, 55555, -11111, -22222, -33333, -44444, 'ff99a408-78bb-4939-93cc-65e657e347c6', '1991-06-28', '2007-02-28 23:42:25', 'dictionary', 33.333, 3222193713.7, to_decimal32('5e-5', 5), to_decimal64('5e-15', 15), toDecimal128('5e-35', 35), 1);


SELECT  array_distinct(group_array(dictGetUInt8('db_01501.cache_dict', 'UInt8_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetUInt16('db_01501.cache_dict', 'UInt16_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetUInt32('db_01501.cache_dict', 'UInt32_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetUInt64('db_01501.cache_dict', 'UInt64_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetInt8('db_01501.cache_dict', 'Int8_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetInt16('db_01501.cache_dict', 'Int16_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetInt32('db_01501.cache_dict', 'Int32_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetInt64('db_01501.cache_dict', 'Int64_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetFloat32('db_01501.cache_dict', 'Float32_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetFloat64('db_01501.cache_dict', 'Float64_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGet('db_01501.cache_dict', 'Decimal32_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGet('db_01501.cache_dict', 'Decimal64_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGet('db_01501.cache_dict', 'Decimal128_', to_uint64(number)))) from numbers(10);
system reload dictionaries;
SELECT  array_distinct(group_array(dictGetString('db_01501.cache_dict', 'String_', to_uint64(number)))) from numbers(10);



SELECT  array_distinct(group_array(dictGetUInt8('db_01501.cache_dict', 'UInt8_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetUInt16('db_01501.cache_dict', 'UInt16_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetUInt32('db_01501.cache_dict', 'UInt32_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetUInt64('db_01501.cache_dict', 'UInt64_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetInt8('db_01501.cache_dict', 'Int8_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetInt16('db_01501.cache_dict', 'Int16_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetInt32('db_01501.cache_dict', 'Int32_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetInt64('db_01501.cache_dict', 'Int64_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetFloat32('db_01501.cache_dict', 'Float32_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetFloat64('db_01501.cache_dict', 'Float64_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGet('db_01501.cache_dict', 'Decimal32_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGet('db_01501.cache_dict', 'Decimal64_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGet('db_01501.cache_dict', 'Decimal128_', to_uint64(number)))) from numbers(10);
SELECT  array_distinct(group_array(dictGetString('db_01501.cache_dict', 'String_', to_uint64(number)))) from numbers(10);


system reload dictionaries;


SELECT group_array(dictHas('db_01501.cache_dict', to_uint64(number))) from numbers(10);
SELECT group_array(dictHas('db_01501.cache_dict', to_uint64(number))) from numbers(10);
SELECT group_array(dictHas('db_01501.cache_dict', to_uint64(number))) from numbers(10);
SELECT group_array(dictHas('db_01501.cache_dict', to_uint64(number))) from numbers(10);
SELECT group_array(dictHas('db_01501.cache_dict', to_uint64(number))) from numbers(10);

drop dictionary db_01501.cache_dict;
drop stream db_01501.table_cache_dict;
drop database if exists db_01501;
