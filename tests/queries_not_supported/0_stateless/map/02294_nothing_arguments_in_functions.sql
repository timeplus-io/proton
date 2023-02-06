select array_map(x -> 2 * x, []);
select to_type_name(array_map(x -> 2 * x, []));
select array_map((x, y) -> x + y, [], []);
select to_type_name(array_map((x, y) -> x + y, [], []));
select array_map((x, y) -> x + y, [], CAST([], 'array(int32)'));
select to_type_name(array_map((x, y) -> x + y, [], CAST([], 'array(int32)')));

select arrayFilter(x -> 2 * x < 0, []);
select to_type_name(arrayFilter(x -> 2 * x < 0, []));

select to_type_name(array_map(x -> CAST(x, 'string'), []));
select to_type_name(array_map(x -> to_int32(x), []));
select toColumnTypeName(array_map(x -> to_int32(x), []));

select to_type_name(array_map(x -> [x], []));
select toColumnTypeName(array_map(x -> [x], []));

select to_type_name(array_map(x ->map(1, x), []));
select toColumnTypeName(array_map(x -> map(1, x), []));

select to_type_name(array_map(x ->tuple(x), []));
select toColumnTypeName(array_map(x -> tuple(1, x), []));

select to_type_name(to_int32(assumeNotNull(materialize(NULL))));
select toColumnTypeName(to_int32(assumeNotNull(materialize(NULL))));

select to_type_name(assumeNotNull(materialize(NULL)));
select toColumnTypeName(assumeNotNull(materialize(NULL)));

select to_type_name([assumeNotNull(materialize(NULL))]);
select toColumnTypeName([assumeNotNull(materialize(NULL))]);

select to_type_name(map(1, assumeNotNull(materialize(NULL))));
select toColumnTypeName(map(1, assumeNotNull(materialize(NULL))));

select to_type_name(tuple(1, assumeNotNull(materialize(NULL))));
select toColumnTypeName(tuple(1, assumeNotNull(materialize(NULL))));

select to_type_name(assumeNotNull(materialize(NULL)) * 2);
select toColumnTypeName(assumeNotNull(materialize(NULL)) * 2);
