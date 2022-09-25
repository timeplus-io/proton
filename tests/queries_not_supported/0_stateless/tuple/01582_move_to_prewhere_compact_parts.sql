DROP STREAM IF EXISTS prewhere_move;
create stream prewhere_move (x int, y string) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, to_string(number) FROM numbers(1000);

EXPLAIN SYNTAX SELECT * FROM prewhere_move WHERE x > 100;

DROP STREAM prewhere_move;

create stream prewhere_move (x1 int, x2 int, x3 int, x4 int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere_move SELECT number, number, number, number FROM numbers(1000);

-- Not all conditions moved
EXPLAIN SYNTAX SELECT * FROM prewhere_move WHERE x1 > 100 AND x2 > 100 AND x3 > 100 AND x4 > 100;

DROP STREAM prewhere_move;
