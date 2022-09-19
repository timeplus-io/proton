-- Tags: no-replicated-database

DROP STREAM IF EXISTS data_null;
DROP STREAM IF EXISTS set_null;
DROP STREAM IF EXISTS cannot_be_nullable;

SET data_type_default_nullable='false';

create stream data_null (
    a INT NULL,
    b INT NOT NULL,
    c Nullable(INT),
    d INT
) engine=Memory();


INSERT INTO data_null VALUES (NULL, 2, NULL, 4);

SELECT to_type_name(a), to_type_name(b), to_type_name(c), to_type_name(d) FROM data_null;

SHOW create stream data_null;

create stream data_null_error (
    a Nullable(INT) NULL,
    b INT NOT NULL,
    c Nullable(INT)
) engine=Memory();  --{serverError 377}


create stream data_null_error (
    a INT NULL,
    b Nullable(INT) NOT NULL,
    c Nullable(INT)
) engine=Memory();  --{serverError 377}

SET data_type_default_nullable='true';

create stream set_null (
    a INT NULL,
    b INT NOT NULL,
    c Nullable(INT),
    d INT
) engine=Memory();


INSERT INTO set_null VALUES (NULL, 2, NULL, NULL);

SELECT to_type_name(a), to_type_name(b), to_type_name(c), to_type_name(d) FROM set_null;

SHOW create stream set_null;
DETACH TABLE set_null;
ATTACH TABLE set_null;
SHOW create stream set_null;

create stream cannot_be_nullable (n int8, a array(uint8)) ENGINE=Memory; -- { serverError 43 }
create stream cannot_be_nullable (n int8, a array(uint8) NOT NULL) ENGINE=Memory;
SHOW create stream cannot_be_nullable;
DETACH TABLE cannot_be_nullable;
ATTACH TABLE cannot_be_nullable;
SHOW create stream cannot_be_nullable;

DROP STREAM data_null;
DROP STREAM set_null;
DROP STREAM cannot_be_nullable;
