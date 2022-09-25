SELECT has([(1, 2), (3, 4)], (to_uint16(3), 4));
SELECT has_any([(1, 2), (3, 4)], [(to_uint16(3), 4)]);
SELECT has_all([(1, 2), (3, 4)], [(to_nullable(1), to_uint64(2)), (to_uint16(3), 4)]);
