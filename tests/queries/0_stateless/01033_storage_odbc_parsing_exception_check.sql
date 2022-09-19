-- Tags: no-fasttest

DROP STREAM IF EXISTS BannerDict;

create stream BannerDict (`BannerID` uint64, `CompaignID` uint64) ENGINE = ODBC('DSN=pgconn;Database=postgres', bannerdict); -- {serverError 42}

create stream BannerDict (`BannerID` uint64, `CompaignID` uint64) ENGINE = ODBC('DSN=pgconn;Database=postgres', somedb, bannerdict);

SHOW create stream BannerDict;

DROP STREAM IF EXISTS BannerDict;
