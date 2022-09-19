-- Tags: no-fasttest

SELECT toUInt256(123) IN (NULL);
SELECT toUInt256(123) AS k GROUP BY k;
SELECT toUInt256(123) AS k FROM system.one INNER JOIN (SELECT toUInt256(123) AS k) t USING k;
SELECT array_enumerate_uniq([toUInt256(123), toUInt256(456), toUInt256(123)]);

SELECT toInt256(123) IN (NULL);
SELECT toInt256(123) AS k GROUP BY k;
SELECT toInt256(123) AS k FROM system.one INNER JOIN (SELECT toInt256(123) AS k) t USING k;
SELECT array_enumerate_uniq([toInt256(123), toInt256(456), toInt256(123)]);

-- SELECT toUInt128(123) IN (NULL);
-- SELECT toUInt128(123) AS k GROUP BY k;
-- SELECT toUInt128(123) AS k FROM system.one INNER JOIN (SELECT toUInt128(123) AS k) t USING k;
-- SELECT array_enumerate_uniq([toUInt128(123), toUInt128(456), toUInt128(123)]);

SELECT to_int128(123) IN (NULL);
SELECT to_int128(123) AS k GROUP BY k;
SELECT to_int128(123) AS k FROM system.one INNER JOIN (SELECT to_int128(123) AS k) t USING k;
SELECT array_enumerate_uniq([to_int128(123), to_int128(456), to_int128(123)]);

SELECT toNullable(toUInt256(321)) IN (NULL);
SELECT toNullable(toUInt256(321)) AS k GROUP BY k;
SELECT toNullable(toUInt256(321)) AS k FROM system.one INNER JOIN (SELECT toUInt256(321) AS k) t USING k;
SELECT array_enumerate_uniq([toNullable(toUInt256(321)), toNullable(toUInt256(456)), toNullable(toUInt256(321))]);

SELECT toNullable(toInt256(321)) IN (NULL);
SELECT toNullable(toInt256(321)) AS k GROUP BY k;
SELECT toNullable(toInt256(321)) AS k FROM system.one INNER JOIN (SELECT toInt256(321) AS k) t USING k;
SELECT array_enumerate_uniq([toNullable(toInt256(321)), toNullable(toInt256(456)), toNullable(toInt256(321))]);

-- SELECT toNullable(toUInt128(321)) IN (NULL);
-- SELECT toNullable(toUInt128(321)) AS k GROUP BY k;
-- SELECT toNullable(toUInt128(321)) AS k FROM system.one INNER JOIN (SELECT toUInt128(321) AS k) t USING k;
-- SELECT array_enumerate_uniq([toNullable(toUInt128(321)), toNullable(toUInt128(456)), toNullable(toUInt128(321))]);

SELECT toNullable(to_int128(321)) IN (NULL);
SELECT toNullable(to_int128(321)) AS k GROUP BY k;
SELECT toNullable(to_int128(321)) AS k FROM system.one INNER JOIN (SELECT to_int128(321) AS k) t USING k;
SELECT array_enumerate_uniq([toNullable(to_int128(321)), toNullable(to_int128(456)), toNullable(to_int128(321))]);
