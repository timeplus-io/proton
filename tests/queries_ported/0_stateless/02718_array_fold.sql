SELECT '-- Const arrays';
SELECT array_fold( acc,x -> acc+x*2,  [1, 2, 3, 4], to_uint64(3));
SELECT array_fold( acc,x -> acc+x*2,  empty_array_int64(), to_int64(3));
SELECT array_fold( acc,x,y -> acc+x*2+y*3,  [1, 2, 3, 4], [5, 6, 7, 8], to_uint64(3));
SELECT array_fold( acc,x -> array_push_back(acc, x),  [1, 2, 3, 4], empty_array_int64());
SELECT array_fold( acc,x -> array_push_front(acc, x),  [1, 2, 3, 4], empty_array_int64());
SELECT array_fold( acc,x -> (array_push_front(acc.1, x),array_push_back(acc.2, x)),  [1, 2, 3, 4], (empty_array_int64(), empty_array_int64()));
SELECT array_fold( acc,x -> x%2 ? (array_push_back(acc.1, x), acc.2): (acc.1, array_push_back(acc.2, x)),  [1, 2, 3, 4, 5, 6], (empty_array_int64(), empty_array_int64()));

SELECT '-- Non-const arrays';
SELECT array_fold( acc,x -> acc+x,  range(number), number) FROM system.numbers LIMIT 5;
SELECT array_fold( acc,x -> array_push_front(acc,x),  range(number), empty_array_uInt64()) FROM system.numbers LIMIT 5;
SELECT array_fold( acc,x -> x%2 ? array_push_front(acc,x) : array_push_back(acc,x),  range(number), empty_array_uInt64()) FROM system.numbers LIMIT 5;

SELECT '-- Bug 57458';

DROP STREAM IF EXISTS tab;

CREATE STREAM tab (line string, patterns array(string)) ENGINE = MergeTree ORDER BY line;
INSERT INTO tab(line, patterns) VALUES ('abcdef', ['c']), ('ghijkl', ['h', 'k']), ('mnopqr', ['n']);

SELECT
    line,
    patterns,
    array_fold(acc, pat -> position(line, pat), patterns, 0::uint64)
FROM tab
ORDER BY line;

DROP STREAM tab;

CREATE STREAM tab(line string) ENGINE = Memory();
INSERT INTO tab(line) VALUES ('xxx..yyy..'), ('..........'), ('..xx..yyy.'), ('..........'), ('xxx.......');

SELECT
    line,
    split_by_non_alpha(line),
    array_fold(
        (acc, str) -> position(line, str),
        split_by_non_alpha(line),
        0::uint64
    )
FROM
    tab;

DROP STREAM tab;

SELECT ' -- Bug 57816';

SELECT array_fold(acc, x -> array_intersect(acc, x), [['qwe', 'asd'], ['qwe','asde']], []);
