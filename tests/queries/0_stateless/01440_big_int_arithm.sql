select (to_int128(-1) + to_int8(1)) x, (toInt256(-1) + to_int8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int16(1)) x, (toInt256(-1) + to_int16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int32(1)) x, (toInt256(-1) + to_int32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_int64(1)) x, (toInt256(-1) + to_int64(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint8(1)) x, (toInt256(-1) + to_uint8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint16(1)) x, (toInt256(-1) + to_uint16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint32(1)) x, (toInt256(-1) + to_uint32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + to_uint64(1)) x, (toInt256(-1) + to_uint64(1)) y, to_type_name(x), to_type_name(y);

select (to_int128(-1) + to_int128(1)) x, (toInt256(-1) + to_int128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + toInt256(1)) x, (toInt256(-1) + toInt256(1)) y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) + toUInt128(1)) x, (toInt256(-1) + toUInt128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) + toUInt256(1)) x, (toInt256(-1) + toUInt256(1)) y, to_type_name(x), to_type_name(y);


select (to_int128(-1) - to_int8(1)) x, (toInt256(-1) - to_int8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int16(1)) x, (toInt256(-1) - to_int16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int32(1)) x, (toInt256(-1) - to_int32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_int64(1)) x, (toInt256(-1) - to_int64(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint8(1)) x, (toInt256(-1) - to_uint8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint16(1)) x, (toInt256(-1) - to_uint16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint32(1)) x, (toInt256(-1) - to_uint32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - to_uint64(1)) x, (toInt256(-1) - to_uint64(1)) y, to_type_name(x), to_type_name(y);

select (to_int128(-1) - to_int128(1)) x, (toInt256(-1) - to_int128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - toInt256(1)) x, (toInt256(-1) - toInt256(1)) y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) - toUInt128(1)) x, (toInt256(-1) - toUInt128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) - toUInt256(1)) x, (toInt256(-1) - toUInt256(1)) y, to_type_name(x), to_type_name(y);


select (to_int128(-1) * to_int8(1)) x, (toInt256(-1) * to_int8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int16(1)) x, (toInt256(-1) * to_int16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int32(1)) x, (toInt256(-1) * to_int32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_int64(1)) x, (toInt256(-1) * to_int64(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint8(1)) x, (toInt256(-1) * to_uint8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint16(1)) x, (toInt256(-1) * to_uint16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint32(1)) x, (toInt256(-1) * to_uint32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * to_uint64(1)) x, (toInt256(-1) * to_uint64(1)) y, to_type_name(x), to_type_name(y);

select (to_int128(-1) * to_int128(1)) x, (toInt256(-1) * to_int128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * toInt256(1)) x, (toInt256(-1) * toInt256(1)) y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) * toUInt128(1)) x, (toInt256(-1) * toUInt128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) * toUInt256(1)) x, (toInt256(-1) * toUInt256(1)) y, to_type_name(x), to_type_name(y);


select int_div(to_int128(-1), to_int8(-1)) x, int_div(toInt256(-1), to_int8(-1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int16(-1)) x, int_div(toInt256(-1), to_int16(-1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int32(-1)) x, int_div(toInt256(-1), to_int32(-1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_int64(-1)) x, int_div(toInt256(-1), to_int64(-1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint8(1)) x, int_div(toInt256(-1), to_uint8(1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint16(1)) x, int_div(toInt256(-1), to_uint16(1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint32(1)) x, int_div(toInt256(-1), to_uint32(1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), to_uint64(1)) x, int_div(toInt256(-1), to_uint64(1)) y, to_type_name(x), to_type_name(y);

select int_div(to_int128(-1), to_int128(-1)) x, int_div(toInt256(-1), to_int128(-1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), toInt256(-1)) x, int_div(toInt256(-1), toInt256(-1)) y, to_type_name(x), to_type_name(y);
--select int_div(to_int128(-1), toUInt128(1)) x, int_div(toInt256(-1), toUInt128(1)) y, to_type_name(x), to_type_name(y);
select int_div(to_int128(-1), toUInt256(1)) x, int_div(toInt256(-1), toUInt256(1)) y, to_type_name(x), to_type_name(y);


select (to_int128(-1) / to_int8(-1)) x, (toInt256(-1) / to_int8(-1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int16(-1)) x, (toInt256(-1) / to_int16(-1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int32(-1)) x, (toInt256(-1) / to_int32(-1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_int64(-1)) x, (toInt256(-1) / to_int64(-1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint8(1)) x, (toInt256(-1) / to_uint8(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint16(1)) x, (toInt256(-1) / to_uint16(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint32(1)) x, (toInt256(-1) / to_uint32(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / to_uint64(1)) x, (toInt256(-1) / to_uint64(1)) y, to_type_name(x), to_type_name(y);

select (to_int128(-1) / to_int128(-1)) x, (toInt256(-1) / to_int128(-1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / toInt256(-1)) x, (toInt256(-1) / toInt256(-1)) y, to_type_name(x), to_type_name(y);
--select (to_int128(-1) / toUInt128(1)) x, (toInt256(-1) / toUInt128(1)) y, to_type_name(x), to_type_name(y);
select (to_int128(-1) / toUInt256(1)) x, (toInt256(-1) / toUInt256(1)) y, to_type_name(x), to_type_name(y);
