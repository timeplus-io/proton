select [(to_uint8(3), to_uint8(3))] = [(to_int16(3), to_int16(3))];
select has_any([(to_int16(3), to_int16(3))],[(to_int16(3), to_int16(3))]);
select array_filter(x -> x = (to_int16(3), to_int16(3)), array_zip([to_uint8(3)], [to_uint8(3)]));
select has_any([(to_uint8(3), to_uint8(3))],[(to_int16(3), to_int16(3))]);
