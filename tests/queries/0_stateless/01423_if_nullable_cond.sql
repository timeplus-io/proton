SELECT CAST(null, 'Nullable(uint8)') = 1 ? CAST(null, 'Nullable(uint8)') : -1 AS x, to_type_name(x), dumpColumnStructure(x);
