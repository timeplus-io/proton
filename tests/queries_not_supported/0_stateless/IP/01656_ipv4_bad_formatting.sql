SELECT array_join(['1.1.1.1', '255.255.255.255']) AS x, toIPv4(x) AS y, to_uint32(y) AS z FORMAT PrettyCompactNoEscapes;
