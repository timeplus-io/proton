SELECT uniq(x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x) FROM (SELECT array_join([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniq(x, x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x, array_map(elem -> [elem, elem], x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x, to_string(x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq((x, x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq((x, array_map(elem -> [elem, elem], x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq((x, to_string(x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x) FROM (SELECT array_join([[], ['a'], ['a', NULL, 'b'], []]) AS x);

SELECT uniqExact(x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x) FROM (SELECT array_join([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniqExact(x, x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x, array_map(elem -> [elem, elem], x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x, to_string(x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact((x, x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact((x, array_map(elem -> [elem, elem], x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact((x, to_string(x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x) FROM (SELECT array_join([[], ['a'], ['a', NULL, 'b'], []]) AS x);

SELECT uniqUpTo(3)(x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x) FROM (SELECT array_join([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniqUpTo(3)(x, x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x, array_map(elem -> [elem, elem], x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x, to_string(x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)((x, x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)((x, array_map(elem -> [elem, elem], x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)((x, to_string(x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x) FROM (SELECT array_join([[], ['a'], ['a', NULL, 'b'], []]) AS x);
