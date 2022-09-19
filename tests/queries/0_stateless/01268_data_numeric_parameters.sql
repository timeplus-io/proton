DROP STREAM IF EXISTS ints;
DROP STREAM IF EXISTS floats;
DROP STREAM IF EXISTS strings;

create stream ints (
    a TINYINT,
    b TINYINT(8),
    c SMALLINT,
    d SMALLINT(16),
    e INT,
    f INT(32),
    g BIGINT,
    h BIGINT(64)
) engine=Memory;

INSERT INTO ints VALUES (1, 8, 11, 16, 21, 32, 41, 64);

SELECT  to_type_name(a), to_type_name(b), to_type_name(c), to_type_name(d), to_type_name(e), to_type_name(f), to_type_name(g), to_type_name(h) FROM ints;

create stream floats (
    a FLOAT,
    b FLOAT(12),
    c FLOAT(15, 22),
    d DOUBLE,
    e DOUBLE(12),
    f DOUBLE(4, 18)

) engine=Memory;

INSERT INTO floats VALUES (1.1, 1.2, 1.3, 41.1, 41.1, 42.1);

SELECT  to_type_name(a), to_type_name(b), to_type_name(c), to_type_name(d), to_type_name(e), to_type_name(f) FROM floats;


create stream strings (
    a VARCHAR,
    b VARCHAR(11)
) engine=Memory;

INSERT INTO strings VALUES ('test', 'string');

SELECT  to_type_name(a), to_type_name(b)  FROM strings;

DROP STREAM floats;
DROP STREAM ints;
DROP STREAM strings;
