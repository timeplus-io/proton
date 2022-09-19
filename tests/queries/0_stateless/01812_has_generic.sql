SELECT has([(1, 2), (3, 4)], (to_uint16(3), 4));
SELECT hasAny([(1, 2), (3, 4)], [(to_uint16(3), 4)]);
SELECT hasAll([(1, 2), (3, 4)], [(toNullable(1), to_uint64(2)), (to_uint16(3), 4)]);
