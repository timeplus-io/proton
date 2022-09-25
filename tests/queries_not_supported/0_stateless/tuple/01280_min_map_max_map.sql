select minMap([to_int32(number % 10), number % 10 + 1], [number, 1]) as m, to_type_name(m) from numbers(1, 100);
select minMap([1], [to_int32(number) - 50]) from numbers(1, 100);
select minMap([cast(1, 'Decimal(10, 2)')], [cast(to_int32(number) - 50, 'Decimal(10, 2)')]) from numbers(1, 100);

select maxMap([to_int32(number % 10), number % 10 + 1], [number, 1]) as m, to_type_name(m) from numbers(1, 100);
select maxMap([1], [to_int32(number) - 50]) from numbers(1, 100);
select maxMap([cast(1, 'Decimal(10, 2)')], [cast(to_int32(number) - 50, 'Decimal(10, 2)')]) from numbers(1, 100);

-- check different types for minMap
select minMap(val, cnt) from values ('val array(UUID), cnt array(UUID)',
	(['01234567-89ab-cdef-0123-456789abcdef'], ['01111111-89ab-cdef-0123-456789abcdef']),
	(['01234567-89ab-cdef-0123-456789abcdef'], ['02222222-89ab-cdef-0123-456789abcdef']));
select minMap(val, cnt) from values ('val array(string), cnt array(string)',  (['1'], ['1']), (['1'], ['2']));
select minMap(val, cnt) from values ('val array(FixedString(1)), cnt array(FixedString(1))',  (['1'], ['1']), (['1'], ['2']));
select minMap(val, cnt) from values ('val array(uint64), cnt array(uint64)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val array(float64), cnt array(int8)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val array(date), cnt array(int16)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val array(datetime(\'Europe/Moscow\')), cnt array(int32)',  ([1], [1]), ([1], [2]));
select minMap(val, cnt) from values ('val array(Decimal(10, 2)), cnt array(int16)',  (['1.01'], [1]), (['1.01'], [2]));
select minMap(val, cnt) from values ('val array(Enum16(\'a\'=1)), cnt array(int16)',  (['a'], [1]), (['a'], [2]));

-- check different types for maxMap
select maxMap(val, cnt) from values ('val array(UUID), cnt array(UUID)',
	(['01234567-89ab-cdef-0123-456789abcdef'], ['01111111-89ab-cdef-0123-456789abcdef']),
	(['01234567-89ab-cdef-0123-456789abcdef'], ['02222222-89ab-cdef-0123-456789abcdef']));
select maxMap(val, cnt) from values ('val array(string), cnt array(string)',  (['1'], ['1']), (['1'], ['2']));
select maxMap(val, cnt) from values ('val array(FixedString(1)), cnt array(FixedString(1))',  (['1'], ['1']), (['1'], ['2']));
select maxMap(val, cnt) from values ('val array(uint64), cnt array(uint64)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val array(float64), cnt array(int8)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val array(date), cnt array(int16)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val array(datetime(\'Europe/Moscow\')), cnt array(int32)',  ([1], [1]), ([1], [2]));
select maxMap(val, cnt) from values ('val array(Decimal(10, 2)), cnt array(int16)',  (['1.01'], [1]), (['1.01'], [2]));
select maxMap(val, cnt) from values ('val array(Enum16(\'a\'=1)), cnt array(int16)',  (['a'], [1]), (['a'], [2]));

-- bugfix, minMap and maxMap should not remove values with zero and empty strings but this behavior should not affect sumMap
select minMap(val, cnt) from values ('val array(uint64), cnt array(uint64)',  ([1], [0]), ([2], [0]));
select maxMap(val, cnt) from values ('val array(uint64), cnt array(uint64)',  ([1], [0]), ([2], [0]));
select minMap(val, cnt) from values ('val array(string), cnt array(string)',  (['A'], ['']), (['B'], ['']));
select maxMap(val, cnt) from values ('val array(string), cnt array(string)',  (['A'], ['']), (['B'], ['']));
select sumMap(val, cnt) from values ('val array(uint64), cnt array(uint64)',  ([1], [0]), ([2], [0]));

-- check working with arrays and tuples as values
select minMap([1, 1, 1], [[1, 2], [1], [1, 2, 3]]);
select maxMap([1, 1, 1], [[1, 2], [1], [1, 2, 3]]);
select minMap([1, 1, 1], [(1, 2), (1, 1), (1, 3)]);
select maxMap([1, 1, 1], [(1, 2), (1, 1), (1, 3)]);
