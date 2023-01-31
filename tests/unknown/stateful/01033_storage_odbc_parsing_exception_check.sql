-- Tags: no-fasttest

DROP STREAM IF EXISTS BannerDict;

CREATE STREAM BannerDict (`BannerID` UInt64, `CompaignID` UInt64) ENGINE = ODBC('DSN=pgconn;Database=postgres', bannerdict); -- {serverError 42}

CREATE STREAM BannerDict (`BannerID` UInt64, `CompaignID` UInt64) ENGINE = ODBC('DSN=pgconn;Database=postgres', somedb, bannerdict);

SHOW CREATE STREAM BannerDict;

DROP STREAM IF EXISTS BannerDict;
