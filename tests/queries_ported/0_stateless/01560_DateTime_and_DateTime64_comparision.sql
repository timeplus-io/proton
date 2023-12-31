SELECT
    n,
    to_type_name(dt64) AS dt64_typename,

    '<',
    dt64 < dt,
    to_datetime(dt64) < dt,
    dt64 < to_datetime64(dt, 1, 'UTC'),
    
    '<=',
    dt64 <= dt,
    to_datetime(dt64) <= dt,
    dt64 <= to_datetime64(dt, 1, 'UTC'),

    '=',
    dt64 = dt,
    to_datetime(dt64) = dt,
    dt64 = to_datetime64(dt, 1, 'UTC'),
    
    '>=',
    dt64 >= dt,
    to_datetime(dt64) >= dt,
    dt64 >= to_datetime64(dt, 1, 'UTC'),
    
    '>',
    dt64 > dt,
    to_datetime(dt64) > dt,
    dt64 > to_datetime64(dt, 1, 'UTC'),

    '!=',
    dt64 != dt,
    to_datetime(dt64) != dt,
    dt64 != to_datetime64(dt, 1, 'UTC')
FROM
(
    WITH to_datetime('2015-05-18 07:40:11') as value
    SELECT
        number - 1 as n,
        to_datetime64(value, 1, 'UTC') AS dt64,
        value - n as dt
    FROM system.numbers
    LIMIT 3
)
