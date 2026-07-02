-- Check if we catch overflow on num_rows

-- data_size = 4, data = AAAA, num_rows = 2^63
SELECT hex(group_concat_merge(',', 10)(state))
FROM
(
    SELECT CAST(unhex('044141414180808080808080808001'), 'aggregate_function(group_concat(\',\', 10), string)') AS state
) -- { serverError BAD_ARGUMENTS }
