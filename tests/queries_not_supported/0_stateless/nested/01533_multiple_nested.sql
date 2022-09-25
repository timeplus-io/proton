DROP STREAM IF EXISTS nested;

SET flatten_nested = 0;

create stream nested
(
    col1 nested(a uint32, s string),
    col2 nested(a uint32, n nested(s string, b uint32)),
    col3 nested(n1 nested(a uint32, b uint32), n2 nested(s string, t string))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO nested VALUES ([(1, 'q'), (2, 'w'), (3, 'e')], [(4, [('a', 5), ('s', 6), ('d', 7)])], [([(8, 9), (10, 11)], [('z', 'x'), ('c', 'v')])]);
INSERT INTO nested VALUES ([(12, 'qq')], [(4, []), (5, [('b', 6), ('n', 7)])], [([], []), ([(44, 55), (66, 77)], [])]);

OPTIMIZE STREAM nested FINAL;

SELECT 'all';
SELECT * FROM nested;
SELECT 'col1';
SELECT col1.a, col1.s FROM nested;
SELECT 'col2';
SELECT col2.a, col2.n, col2.n.s, col2.n.b FROM nested;
SELECT 'col3';
SELECT col3.n1, col3.n2, col3.n1.a, col3.n1.b, col3.n2.s, col3.n2.t FROM nested;

SELECT 'read files';

SYSTEM DROP MARK CACHE;
SELECT col1.a FROM nested FORMAT Null;

-- 4 files: (col1.size0, col1.a) x2
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (lower(query) LIKE lower('SELECT col1.a FROM %nested%'))
    AND event_date >= yesterday() AND current_database = currentDatabase();

SYSTEM DROP MARK CACHE;
SELECT col3.n2.s FROM nested FORMAT Null;

-- 6 files: (col3.size0, col3.n2.size1, col3.n2.s) x2
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (lower(query) LIKE lower('SELECT col3.n2.s FROM %nested%'))
    AND event_date >= yesterday() AND current_database = currentDatabase();

DROP STREAM nested;

create stream nested
(
    id uint32,
    col1 nested(a uint32, n nested(s string, b uint32))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO nested SELECT number, array_map(x -> (x, array_map(y -> (to_string(y * x), y + x), range(number % 17))), range(number % 19)) FROM numbers(100000);
SELECT id % 10, sum(length(col1)), sumArray(array_map(x -> length(x), col1.n.b)) FROM nested GROUP BY id % 10;

SELECT array_sum(col1.a), array_map(x -> x * x * 2, col1.a) FROM nested ORDER BY id LIMIT 5;
SELECT untuple(array_join(array_join(col1.n))) FROM nested ORDER BY id LIMIT 10 OFFSET 10;

DROP STREAM nested;
