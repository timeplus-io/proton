SELECT 'int8';
SELECT to_int8(0), bitPositionsToArray(to_int8(0));
SELECT to_int8(1), bitPositionsToArray(to_int8(1));
SELECT to_int8(-1), bitPositionsToArray(to_int8(-1));
SELECT to_int8(127), bitPositionsToArray(to_int8(127));
SELECT to_int8(128), bitPositionsToArray(to_int8(128));

SELECT 'int16';
SELECT to_int16(0), bitPositionsToArray(to_int16(0));
SELECT to_int16(1), bitPositionsToArray(to_int16(1));
SELECT to_int16(-1), bitPositionsToArray(to_int16(-1));
select to_int16(32765), bitPositionsToArray(to_int16(32765));
select to_int16(32768), bitPositionsToArray(to_int16(32768));

SELECT 'int32';
SELECT to_int32(0), bitPositionsToArray(to_int32(0));
SELECT to_int32(1), bitPositionsToArray(to_int32(1));

SELECT 'int64';
SELECT to_int64(0), bitPositionsToArray(to_int64(0));
SELECT to_int64(1), bitPositionsToArray(to_int64(1));

SELECT 'int128';
SELECT to_int128(0), bitPositionsToArray(to_int128(0));
SELECT to_int128(1), bitPositionsToArray(to_int128(1));

SELECT 'int256';
SELECT to_int256(0), bitPositionsToArray(to_int256(0));
SELECT to_int256(1), bitPositionsToArray(to_int256(1));

SELECT 'uint8';
SELECT to_uint8(0), bitPositionsToArray(to_uint8(0));
SELECT to_uint8(1), bitPositionsToArray(to_uint8(1));
SELECT to_uint8(128), bitPositionsToArray(to_uint8(128));

SELECT 'uint16';
SELECT to_uint16(0), bitPositionsToArray(to_uint16(0));
SELECT to_uint16(1), bitPositionsToArray(to_uint16(1));

SELECT 'uint32';
SELECT to_uint32(0), bitPositionsToArray(to_uint32(0));
SELECT to_uint32(1), bitPositionsToArray(to_uint32(1));

SELECT 'uint64';
SELECT to_uint64(0), bitPositionsToArray(to_uint64(0));
SELECT to_uint64(1), bitPositionsToArray(to_uint64(1));

SELECT 'uint128';
SELECT to_uint128(0), bitPositionsToArray(to_uint128(0));
SELECT to_uint128(1), bitPositionsToArray(to_uint128(1));
SELECT to_uint128(-1), bitPositionsToArray(to_uint128(1));

SELECT 'uint256';
SELECT to_uint256(0), bitPositionsToArray(to_uint256(0));
SELECT to_uint256(1), bitPositionsToArray(to_uint256(1));
