select group_array(s) from (select sum(n) s from (select to_decimal32(1, 2) as n));
select group_array(s) from (select sum(n) s from (select to_decimal64(1, 5) as n));
select group_array(s) from (select sum(n) s from (select toDecimal128(1, 10) as n));

select group_array(s) from (select sum(n) s from (select to_decimal32(number, 2) as n from numbers(1000)));
select group_array(s) from (select sum(n) s from (select to_decimal64(number, 5) as n from numbers(1000)));
select group_array(s) from (select sum(n) s from (select toDecimal128(number, 10) as n from numbers(1000)));

DROP STREAM IF EXISTS sensor_value;
create stream sensor_value (
    received_at datetime('Europe/Moscow'),
    device_id uuid,
    sensor_id uuid,
    value nullable(Decimal(18, 4)),
    low_warning nullable(Decimal(18, 4)),
    low_critical nullable(Decimal(18, 4)),
    high_warning nullable(Decimal(18, 4)),
    high_critical nullable(Decimal(18, 4))
) ENGINE = MergeTree
PARTITION BY to_date(received_at)
ORDER BY (device_id, sensor_id);

INSERT INTO sensor_value (received_at, device_id, sensor_id, value, low_warning, low_critical, high_warning, high_critical) VALUES ('2018-12-18 00:16:07', 'a4d92414-09aa-4dbd-80b2-124ddaacf333', 'ed87e57c-9331-462a-80b4-9f0c005e88c8', '0.4400', '-10000000.0000', '-10000000.0000', '10000000.0000', '10000000.0000');

SELECT `time`, group_array((sensor_id, volume)) AS groupArr FROM (
    SELECT (int_div(to_uint32(received_at), 900) * 900) AS `time`, sensor_id, avg(value) AS volume
    FROM sensor_value
    WHERE received_at BETWEEN '2018-12-12 00:00:00' AND '2018-12-30 00:00:00'
    GROUP BY `time`,sensor_id
    ORDER BY `time`
) GROUP BY `time` ORDER BY `time`;

DROP STREAM sensor_value;

select s.a, s.b, max(s.dt1) dt1, s.c, s.d, s.f, s.i, max(s.dt2) dt2 from (
    select to_uint64(4360430)                   a
        , to_uint64(5681495)                    b
        , to_datetime('2018-11-01 10:44:58', 'Europe/Moscow')    dt1
        , 'txt'                                c
        , toDecimal128('274.350000000000', 12) d
        , toDecimal128(268.970000000000, 12)   f
        , toDecimal128(0.000000000000, 12)     i
        , to_datetime('2018-11-02 00:00:00', 'Europe/Moscow')    dt2
    union all
    select to_uint64(4341757)                   a
        , to_uint64(5657967)                    b
        , to_datetime('2018-11-01 16:47:46', 'Europe/Moscow')    dt1
        , 'txt'                                c
        , toDecimal128('321.380000000000', 12) d
        , toDecimal128(315.080000000000, 12)   f
        , toDecimal128(0.000000000000, 12)     i
        , to_datetime('2018-11-02 00:00:00', 'Europe/Moscow')    dt2
    union all
    select to_uint64(4360430)                   a
        , to_uint64(5681495)                    b
        , to_datetime('2018-11-02 09:00:07', 'Europe/Moscow')    dt1
        , 'txt'                                c
        , toDecimal128('274.350000000000', 12) d
        , toDecimal128(268.970000000000, 12)   f
        , toDecimal128(0.000000000000, 12)     i
        , to_datetime('2018-11-02 00:00:00', 'Europe/Moscow')    dt2
) s group by s.a, s.b, s.c, s.d, s.f, s.i ORDER BY s.a, s.b, s.c, s.d, s.f, s.i;
