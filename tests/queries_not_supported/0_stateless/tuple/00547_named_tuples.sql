SELECT CAST((1, 'Hello') AS tuple(x uint64, s string)) AS t, to_type_name(t), t.1, t.2, tupleElement(t, 'x'), tupleElement(t, 's');
