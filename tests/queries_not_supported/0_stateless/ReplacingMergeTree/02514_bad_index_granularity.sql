CREATE STREAM t
(
    id int64,
    d string,
    p map(string, string)
)
ENGINE = ReplacingMergeTree order by id settings index_granularity = 0; -- { serverError BAD_ARGUMENTS }
