-- Tags: no-fasttest

DROP STREAM IF EXISTS testnull;
CREATE STREAM testnull
(
    `a` nullable(string),
    `b` nullable(string),
    `c` nullable(string)
)
ORDER BY c
SETTINGS index_granularity = 8192, allow_nullable_key=1;

INSERT INTO testnull(b,c) SELECT to_string(rand64()) AS b, to_string(rand64()) AS c FROM numbers(1000000);
SELECT sleep(3) FORMAT Null;
SELECT count(distinct b) FROM table(testnull) GROUP BY a SETTINGS max_memory_usage = 10000000; -- {serverError MEMORY_LIMIT_EXCEEDED}

DROP STREAM testnull;
