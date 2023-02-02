select (to_int128(-1) + to_int8(1)) as x, (to_int256(-1) + to_int8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int16(1)) as x, (to_int256(-1) + to_int16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int32(1)) as x, (to_int256(-1) + to_int32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int64(1)) as x, (to_int256(-1) + to_int64(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint8(1)) as x, (to_int256(-1) + to_uint8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint16(1)) as x, (to_int256(-1) + to_uint16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint32(1)) as x, (to_int256(-1) + to_uint32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint64(1)) as x, (to_int256(-1) + to_uint64(1)) as y, to_type_name(x), to_type_name(y);

select (to_int128(-1) + to_int128(1)) as x, (to_int256(-1) + to_int128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int256(1)) as x, (to_int256(-1) + to_int256(1)) as y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) + to_uint128(1)) as x, (to_int256(-1) + to_uint128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint256(1)) as x, (to_int256(-1) + to_uint256(1)) as y, to_type_name(x), to_type_name(y);


select (to_int128(-1) - to_int8(1)) as x, (to_int256(-1) - to_int8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int16(1)) as x, (to_int256(-1) - to_int16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int32(1)) as x, (to_int256(-1) - to_int32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int64(1)) as x, (to_int256(-1) - to_int64(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint8(1)) as x, (to_int256(-1) - to_uint8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint16(1)) as x, (to_int256(-1) - to_uint16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint32(1)) as x, (to_int256(-1) - to_uint32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint64(1)) as x, (to_int256(-1) - to_uint64(1)) as y, to_type_name(x), to_type_name(y);

select (to_int128(-1) - to_int128(1)) as x, (to_int256(-1) - to_int128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int256(1)) as x, (to_int256(-1) - to_int256(1)) as y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) - to_uint128(1)) as x, (to_int256(-1) - to_uint128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint256(1)) as x, (to_int256(-1) - to_uint256(1)) as y, to_type_name(x), to_type_name(y);


select (to_int128(-1) * to_int8(1)) as x, (to_int256(-1) * to_int8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int16(1)) as x, (to_int256(-1) * to_int16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int32(1)) as x, (to_int256(-1) * to_int32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int64(1)) as x, (to_int256(-1) * to_int64(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint8(1)) as x, (to_int256(-1) * to_uint8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint16(1)) as x, (to_int256(-1) * to_uint16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint32(1)) as x, (to_int256(-1) * to_uint32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint64(1)) as x, (to_int256(-1) * to_uint64(1)) as y, to_type_name(x), to_type_name(y);

select (to_int128(-1) * to_int128(1)) as x, (to_int256(-1) * to_int128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int256(1)) as x, (to_int256(-1) * to_int256(1)) as y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) * to_uint128(1)) as x, (to_int256(-1) * to_uint128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint256(1)) as x, (to_int256(-1) * to_uint256(1)) as y, to_type_name(x), to_type_name(y);


select int_div(to_int128(-1), to_int8(-1)) as x, int_div(to_int256(-1), to_int8(-1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int16(-1)) as x, int_div(to_int256(-1), to_int16(-1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int32(-1)) as x, int_div(to_int256(-1), to_int32(-1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int64(-1)) as x, int_div(to_int256(-1), to_int64(-1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint8(1)) as x, int_div(to_int256(-1), to_uint8(1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint16(1)) as x, int_div(to_int256(-1), to_uint16(1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint32(1)) as x, int_div(to_int256(-1), to_uint32(1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint64(1)) as x, int_div(to_int256(-1), to_uint64(1)) as y, to_type_name(x), to_type_name(y);

select int_div(to_int128(-1), to_int128(-1)) as x, int_div(to_int256(-1), to_int128(-1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int256(-1)) as x, int_div(to_int256(-1), to_int256(-1)) as y, to_type_name(x), to_type_name(y);
--select int_div(to_int128(-1), to_uint128(1)) as x, int_div(to_int256(-1), to_uint128(1)) as y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint256(1)) as x, int_div(to_int256(-1), to_uint256(1)) as y, to_type_name(x), to_type_name(y);


select (to_int128(-1) / to_int8(-1)) as x, (to_int256(-1) / to_int8(-1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int16(-1)) as x, (to_int256(-1) / to_int16(-1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int32(-1)) as x, (to_int256(-1) / to_int32(-1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int64(-1)) as x, (to_int256(-1) / to_int64(-1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint8(1)) as x, (to_int256(-1) / to_uint8(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint16(1)) as x, (to_int256(-1) / to_uint16(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint32(1)) as x, (to_int256(-1) / to_uint32(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint64(1)) as x, (to_int256(-1) / to_uint64(1)) as y, to_type_name(x), to_type_name(y);

select (to_int128(-1) / to_int128(-1)) as x, (to_int256(-1) / to_int128(-1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int256(-1)) as x, (to_int256(-1) / to_int256(-1)) as y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) / to_uint128(1)) as x, (to_int256(-1) / to_uint128(1)) as y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint256(1)) as x, (to_int256(-1) / to_uint256(1)) as y, to_type_name(x), to_type_name(y);
