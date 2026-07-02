-- https://github.com/ClickHouse/ClickHouse/issues/93026
SELECT hex(group_concat_merge(',', 10)(state))
FROM
(
    SELECT CAST(unhex('01580180808080108A80808010'), 'aggregate_function(group_concat(\',\', 10), string)') AS state
); -- { serverError BAD_ARGUMENTS }

-- Check for non-monotonic offsets
SELECT hex(group_concat_merge(',', 10)(state))
FROM
(
    SELECT CAST(unhex('0141010100'), 'aggregate_function(group_concat(\',\', 10), string)') AS state
); -- { serverError BAD_ARGUMENTS }
