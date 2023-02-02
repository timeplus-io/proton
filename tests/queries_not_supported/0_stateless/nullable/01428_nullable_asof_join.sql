SET join_use_nulls = 1;

select 'left asof using';

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt; -- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt; -- { serverError 48 }

select 'left asof on';

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt; -- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
ON a.dt >= b.dt AND a.pk = b.pk
ORDER BY a.dt; -- { serverError 48 }

select 'asof using';

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt; -- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt; -- { serverError 48 }

select 'asof on';

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_uint8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt; -- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt; -- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, to_type_name(a.pk), to_type_name(b.pk), to_type_name(materialize(a.dt)), to_type_name(materialize(b.dt))
FROM (SELECT to_uint8(number) > 0 as pk, to_nullable(to_uint8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, to_nullable(0) as dt) b
ON a.dt >= b.dt AND a.pk = b.pk
ORDER BY a.dt; -- { serverError 48 }

SELECT *
FROM (SELECT NULL AS y, 1 AS x, '2020-01-01 10:10:10' :: DateTime64 AS t) AS t1
ASOF LEFT JOIN (SELECT NULL AS y, 1 AS x, '2020-01-01 10:10:10' :: DateTime64 AS t) AS t2
ON t1.t <= t2.t AND t1.x == t2.x FORMAT Null;
