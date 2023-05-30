-- Tags: no-fasttest

SELECT to_uint256(123) IN (NULL);
SELECT to_uint256(123) AS k GROUP BY k;
SELECT to_uint256(123) AS k FROM system.one INNER JOIN (SELECT to_uint256(123) AS k) as t USING k;
SELECT array_enumerate_uniq([to_uint256(123), to_uint256(456), to_uint256(123)]);

SELECT to_int256(123) IN (NULL);
SELECT to_int256(123) AS k GROUP BY k;
SELECT to_int256(123) AS k FROM system.one INNER JOIN (SELECT to_int256(123) AS k) as t USING k;
SELECT array_enumerate_uniq([to_int256(123), to_int256(456), to_int256(123)]);

-- SELECT to_uint128(123) IN (NULL);
-- SELECT to_uint128(123) AS k GROUP BY k;
-- SELECT to_uint128(123) AS k FROM system.one INNER JOIN (SELECT to_uint128(123) AS k) t USING k;
-- SELECT array_enumerate_uniq([to_uint128(123), to_uint128(456), to_uint128(123)]);

SELECT to_int128(123) IN (NULL);
SELECT to_int128(123) AS k GROUP BY k;
SELECT to_int128(123) AS k FROM system.one INNER JOIN (SELECT to_int128(123) AS k) as t USING k;
SELECT array_enumerate_uniq([to_int128(123), to_int128(456), to_int128(123)]);

SELECT to_nullable(to_uint256(321)) IN (NULL);
SELECT to_nullable(to_uint256(321)) AS k GROUP BY k;
SELECT to_nullable(to_uint256(321)) AS k FROM system.one INNER JOIN (SELECT to_uint256(321) AS k) as t USING k;
SELECT array_enumerate_uniq([to_nullable(to_uint256(321)), to_nullable(to_uint256(456)), to_nullable(to_uint256(321))]);

SELECT to_nullable(to_int256(321)) IN (NULL);
SELECT to_nullable(to_int256(321)) AS k GROUP BY k;
SELECT to_nullable(to_int256(321)) AS k FROM system.one INNER JOIN (SELECT to_int256(321) AS k) as t USING k;
SELECT array_enumerate_uniq([to_nullable(to_int256(321)), to_nullable(to_int256(456)), to_nullable(to_int256(321))]);

-- SELECT to_nullable(to_uint128(321)) IN (NULL);
-- SELECT to_nullable(to_uint128(321)) AS k GROUP BY k;
-- SELECT to_nullable(to_uint128(321)) AS k FROM system.one INNER JOIN (SELECT to_uint128(321) AS k) t USING k;
-- SELECT array_enumerate_uniq([to_nullable(to_uint128(321)), to_nullable(to_uint128(456)), to_nullable(to_uint128(321))]);

SELECT to_nullable(to_int128(321)) IN (NULL);
SELECT to_nullable(to_int128(321)) AS k GROUP BY k;
SELECT to_nullable(to_int128(321)) AS k FROM system.one INNER JOIN (SELECT to_int128(321) AS k) as t USING k;
SELECT array_enumerate_uniq([to_nullable(to_int128(321)), to_nullable(to_int128(456)), to_nullable(to_int128(321))]);
