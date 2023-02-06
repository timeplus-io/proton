select to_start_of_interval(to_datetime64('\0930-12-12 12:12:12.1234567', 3), to_interval_nanosecond(1024)); -- {servererror 407}

SELECT
    to_datetime64(-9223372036854775808, 1048575, to_interval_nanosecond(9223372036854775806), NULL),
    to_start_of_interval(to_datetime64(to_interval_nanosecond(to_interval_nanosecond(257), to_datetime64(tostartofinterval(to_datetime64(null)))), '', 100), to_interval_nanosecond(tostartofinterval(to_datetime64(to_interval_nanosecond(null), null)), -1)),
    to_start_of_interval(to_datetime64('\0930-12-12 12:12:12.1234567', 3), to_interval_nanosecond(1024)); -- {servererror 407}
