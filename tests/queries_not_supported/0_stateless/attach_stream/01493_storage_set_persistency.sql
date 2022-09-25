-- Tags: no-parallel

DROP STREAM IF EXISTS set;
DROP STREAM IF EXISTS number;

create stream number (number uint64) ();
INSERT INTO number values (1);

SELECT '----- Default Settings -----';
create stream set (val uint64) ENGINE = Set();
INSERT INTO set VALUES (1);
DETACH STREAM set;
ATTACH STREAM set;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP STREAM set;

SELECT '----- Settings persistent=1 -----';
create stream set (val uint64) ENGINE = Set() SETTINGS persistent=1;
INSERT INTO set VALUES (1);
DETACH STREAM set;
ATTACH STREAM set;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP STREAM set;

SELECT '----- Settings persistent=0 -----';
create stream set (val uint64) ENGINE = Set() SETTINGS persistent=0;
INSERT INTO set VALUES (1);
DETACH STREAM set;
ATTACH STREAM set;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP STREAM set;
DROP STREAM number;
