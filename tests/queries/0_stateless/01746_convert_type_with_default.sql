select toUInt8OrDefault('1', cast(2 as uint8));
select toUInt8OrDefault('1xx', cast(2 as uint8));
select to_int8OrDefault('-1', cast(-2 as int8));
select to_int8OrDefault('-1xx', cast(-2 as int8));

select toUInt16OrDefault('1', cast(2 as uint16));
select toUInt16OrDefault('1xx', cast(2 as uint16));
select to_int16OrDefault('-1', cast(-2 as Int16));
select to_int16OrDefault('-1xx', cast(-2 as Int16));

select toUInt32OrDefault('1', cast(2 as uint32));
select toUInt32OrDefault('1xx', cast(2 as uint32));
select to_int32OrDefault('-1', cast(-2 as int32));
select to_int32OrDefault('-1xx', cast(-2 as int32));

select toUInt64OrDefault('1', cast(2 as uint64));
select toUInt64OrDefault('1xx', cast(2 as uint64));
select to_int64OrDefault('-1', cast(-2 as int64));
select to_int64OrDefault('-1xx', cast(-2 as int64));

select to_int128OrDefault('-1', cast(-2 as Int128));
select to_int128OrDefault('-1xx', cast(-2 as Int128));

select toUInt256OrDefault('1', cast(2 as UInt256));
select toUInt256OrDefault('1xx', cast(2 as UInt256));
select toInt256OrDefault('-1', cast(-2 as Int256));
select toInt256OrDefault('-1xx', cast(-2 as Int256));

SELECT toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
SELECT toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
