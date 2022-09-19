-- Tags: no-parallel

DROP STREAM IF EXISTS codecs;

-- test what should work

create stream codecs
(
    a uint8 CODEC(LZ4),
    b uint16 CODEC(ZSTD),
    c Float32 CODEC(Gorilla),
    d uint8 CODEC(Delta, LZ4),
    e float64 CODEC(Gorilla, ZSTD),
    f uint32 CODEC(Delta, Delta, Gorilla),
    g DateTime CODEC(DoubleDelta),
    h DateTime64 CODEC(DoubleDelta, LZ4),
    i string CODEC(NONE)
) ENGINE = MergeTree ORDER BY tuple();

DROP STREAM codecs;

-- test what should not work

create stream codecs (a uint8 CODEC(NONE, NONE)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(NONE, LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(LZ4, NONE)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(LZ4, LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(LZ4, ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }
create stream codecs (a uint8 CODEC(LZ4, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError 36 }

-- test that sanity check is not performed in ATTACH query

DROP STREAM IF EXISTS codecs1;
DROP STREAM IF EXISTS codecs2;
DROP STREAM IF EXISTS codecs3;
DROP STREAM IF EXISTS codecs4;
DROP STREAM IF EXISTS codecs5;
DROP STREAM IF EXISTS codecs6;
DROP STREAM IF EXISTS codecs7;
DROP STREAM IF EXISTS codecs8;

SET allow_suspicious_codecs = 1;

create stream codecs1 (a uint8 CODEC(NONE, NONE)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs2 (a uint8 CODEC(NONE, LZ4)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs3 (a uint8 CODEC(LZ4, NONE)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs4 (a uint8 CODEC(LZ4, LZ4)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs5 (a uint8 CODEC(LZ4, ZSTD)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs6 (a uint8 CODEC(Delta)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs7 (a uint8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple();
create stream codecs8 (a uint8 CODEC(LZ4, Delta)) ENGINE = MergeTree ORDER BY tuple();

SET allow_suspicious_codecs = 0;

SHOW create stream codecs1;
SHOW create stream codecs2;
SHOW create stream codecs3;
SHOW create stream codecs4;
SHOW create stream codecs5;
SHOW create stream codecs6;
SHOW create stream codecs7;
SHOW create stream codecs8;

DETACH TABLE codecs1;
DETACH TABLE codecs2;
DETACH TABLE codecs3;
DETACH TABLE codecs4;
DETACH TABLE codecs5;
DETACH TABLE codecs6;
DETACH TABLE codecs7;
DETACH TABLE codecs8;

ATTACH TABLE codecs1;
ATTACH TABLE codecs2;
ATTACH TABLE codecs3;
ATTACH TABLE codecs4;
ATTACH TABLE codecs5;
ATTACH TABLE codecs6;
ATTACH TABLE codecs7;
ATTACH TABLE codecs8;

SHOW create stream codecs1;
SHOW create stream codecs2;
SHOW create stream codecs3;
SHOW create stream codecs4;
SHOW create stream codecs5;
SHOW create stream codecs6;
SHOW create stream codecs7;
SHOW create stream codecs8;

SELECT * FROM codecs1;
SELECT * FROM codecs2;
SELECT * FROM codecs3;
SELECT * FROM codecs4;
SELECT * FROM codecs5;
SELECT * FROM codecs6;
SELECT * FROM codecs7;
SELECT * FROM codecs8;

DROP STREAM codecs1;
DROP STREAM codecs2;
DROP STREAM codecs3;
DROP STREAM codecs4;
DROP STREAM codecs5;
DROP STREAM codecs6;
DROP STREAM codecs7;
DROP STREAM codecs8;
