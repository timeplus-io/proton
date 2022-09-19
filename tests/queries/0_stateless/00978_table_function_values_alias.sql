SELECT x, s, z FROM VALUES('x uint64, s string, z ALIAS concat(to_string(x), \': \', s)', (1, 'hello'), (2, 'world'));
