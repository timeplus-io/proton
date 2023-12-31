SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS decimal;

create stream IF NOT EXISTS decimal
(
    a DECIMAL(9,4) DEFAULT 0,
    b DECIMAL(18,4) DEFAULT a / 2,
    c DECIMAL(38,4) DEFAULT b / 3,
    d MATERIALIZED a + to_decimal32('0.2', 1),
    e ALIAS b * 2,
    f ALIAS c * 6
) ;

DESC STREAM decimal;

INSERT INTO decimal (a) VALUES (0), (1), (2), (3);
SELECT * FROM decimal;
SELECT a, b, c, d, e, f FROM decimal;

DROP STREAM IF EXISTS decimal;
