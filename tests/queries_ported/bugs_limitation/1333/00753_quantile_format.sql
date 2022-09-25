DROP STREAM IF EXISTS datetime;

create stream datetime (d datetime('UTC')) ;
INSERT INTO datetime(d) VALUES(to_datetime('2016-06-15 23:00:00', 'UTC'));

SELECT quantile(0.2)(d) FROM datetime;
SELECT quantiles(0.2)(d) FROM datetime;

SELECT quantileDeterministic(0.2)(d, 1) FROM datetime;
SELECT quantilesDeterministic(0.2)(d, 1) FROM datetime;

SELECT quantileExact(0.2)(d) FROM datetime;
SELECT quantilesExact(0.2)(d) FROM datetime;

SELECT quantileExactWeighted(0.2)(d, 1) FROM datetime;
SELECT quantilesExactWeighted(0.2)(d, 1) FROM datetime;

SELECT quantile_timing(0.2)(d) FROM datetime;
SELECT quantiles_timing(0.2)(d) FROM datetime;

SELECT quantile_timing_weighted(0.2)(d, 1) FROM datetime;
SELECT quantiles_timing_weighted(0.2)(d, 1) FROM datetime;

SELECT quantileTDigest(0.2)(d) FROM datetime;
SELECT quantilesTDigest(0.2)(d) FROM datetime;

SELECT quantileTDigestWeighted(0.2)(d, 1) FROM datetime;
SELECT quantilesTDigestWeighted(0.2)(d, 1) FROM datetime;

SELECT quantileBFloat16(0.2)(d) FROM datetime;
SELECT quantilesBFloat16(0.2)(d) FROM datetime;

SELECT quantileBFloat16Weighted(0.2)(d, 1) FROM datetime;
SELECT quantilesBFloat16Weighted(0.2)(d, 1) FROM datetime;

DROP STREAM datetime;
