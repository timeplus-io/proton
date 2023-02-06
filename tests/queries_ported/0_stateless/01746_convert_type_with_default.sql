select to_uint8_or_default('1', cast(2 as uint8));
select to_uint8_or_default('1xx', cast(2 as uint8));
select to_int8_or_default('-1', cast(-2 as int8));
select to_int8_or_default('-1xx', cast(-2 as int8));

select to_uint16_or_default('1', cast(2 as uint16));
select to_uint16_or_default('1xx', cast(2 as uint16));
select to_int16_or_default('-1', cast(-2 as int16));
select to_int16_or_default('-1xx', cast(-2 as int16));

select to_uint32_or_default('1', cast(2 as uint32));
select to_uint32_or_default('1xx', cast(2 as uint32));
select to_int32_or_default('-1', cast(-2 as int32));
select to_int32_or_default('-1xx', cast(-2 as int32));

select to_uint64_or_default('1', cast(2 as uint64));
select to_uint64_or_default('1xx', cast(2 as uint64));
select to_int64_or_default('-1', cast(-2 as int64));
select to_int64_or_default('-1xx', cast(-2 as int64));

select to_int128_or_default('-1', cast(-2 as int128));
select to_int128_or_default('-1xx', cast(-2 as int128));

select to_uint256_or_default('1', cast(2 as uint256));
select to_uint256_or_default('1xx', cast(2 as uint256));
select to_int256_or_default('-1', cast(-2 as int256));
select to_int256_or_default('-1xx', cast(-2 as int256));

SELECT to_uuid_or_default('61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as uuid));
SELECT to_uuid_or_default('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as uuid));
