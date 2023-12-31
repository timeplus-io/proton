DROP STREAM IF EXISTS test;

create stream test
(
    `Source.C1` array(uint64),
    `Source.C2` array(uint64)
)
ENGINE = MergeTree()
ORDER BY tuple();

SET optimize_move_functions_out_of_any = 1;

SELECT any(array_filter((c, d) -> (4 = d), `Source.C1`, `Source.C2`)[1]) AS x
FROM test
WHERE 0
GROUP BY 42;

DROP STREAM test;
