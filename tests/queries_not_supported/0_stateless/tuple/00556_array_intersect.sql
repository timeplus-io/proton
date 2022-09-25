select arrayIntersect([], []);
select arrayIntersect([1], []);
select arrayIntersect([1], [1]);
select arrayIntersect([1, 2], [1, 3], [2, 3]);
select arrayIntersect([1, 2], [1, 3], [1, 4]);
select arrayIntersect([1, -1], [1]);
select arrayIntersect([1, -1], [Null, 1]);
select arrayIntersect([1, -1, Null], [Null, 1]);
select arrayIntersect(cast([1, 2] as array(Nullable(int8))), [1, 3]);
select arrayIntersect(CAST([1, -1] AS array(Nullable(int8))), [NULL, 1]);
select arrayIntersect([[1, 2], [1, 1]], [[2, 1], [1, 1]]);
select arrayIntersect([[1, 2, Null], [1, 1]], [[-2, 1], [1, 1]]);
select arrayIntersect([(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])]);
select to_type_name(arrayIntersect([(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])]));

