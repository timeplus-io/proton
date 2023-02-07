WITH map(1, 2, 3, NULL) AS m SELECT m[to_nullable(1)], m[to_nullable(2)], m[to_nullable(3)];
WITH map(1, 2, 3, NULL) AS m SELECT m[materialize(to_nullable(1))], m[materialize(to_nullable(2))], m[materialize(to_nullable(3))];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[to_nullable(1)], m[to_nullable(2)], m[to_nullable(3)];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[materialize(to_nullable(1))], m[materialize(to_nullable(2))], m[materialize(to_nullable(3))];

WITH map('a', 2, 'b', NULL) AS m SELECT m[to_nullable('a')], m[to_nullable('b')], m[to_nullable('c')];
WITH map('a', 2, 'b', NULL) AS m SELECT m[materialize(to_nullable('a'))], m[materialize(to_nullable('b'))], m[materialize(to_nullable('c'))];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m[to_nullable('a')], m[to_nullable('b')], m[to_nullable('c')];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m[materialize(to_nullable('a'))], m[materialize(to_nullable('b'))], m[materialize(to_nullable('c'))];

WITH map(1, 2, 3, NULL) AS m SELECT m[1], m[2], m[3];
WITH map(1, 2, 3, NULL) AS m SELECT m[materialize(1)], m[materialize(2)], m[materialize(3)];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[1], m[2], m[3];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[materialize(1)], m[materialize(2)], m[materialize(3)];

WITH map('a', 2, 'b', NULL) AS m SELECT m['a'], m['b'], m['c'];
WITH map('a', 2, 'b', NULL) AS m SELECT m[materialize('a')], m[materialize('b')], m[materialize('c')];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m['a'], m['b'], m['c'];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m[materialize('a')], m[materialize('b')], m[materialize('c')];
