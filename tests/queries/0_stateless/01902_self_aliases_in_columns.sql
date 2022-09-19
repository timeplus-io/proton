create stream a
(
    `number` uint64,
    `x` MATERIALIZED x
)
ENGINE = MergeTree
ORDER BY number; --{ serverError 174}

create stream foo
(
    i int32,
    j ALIAS j + 1
)
ENGINE = MergeTree() ORDER BY i; --{ serverError 174}
