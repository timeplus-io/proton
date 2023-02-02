DROP STREAM IF EXISTS to_uuid_test;

SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0T'); --{serverError 6}
SELECT to_uuid_or_null('61f0c404-5cb3-11e7-907b-a6006ad3dba0T');
SELECT to_uuid_or_zero('59f0c404-5cb3-11e7-907b-a6006ad3dba0T');

CREATE STREAM to_uuid_test (value string) ENGINE = TinyLog();

INSERT INTO to_uuid_test VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid(value) FROM to_uuid_test; 

INSERT INTO to_uuid_test VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0T');
SELECT to_uuid(value) FROM to_uuid_test; -- {serverError 6}
SELECT to_uuid_or_null(value) FROM to_uuid_test;
SELECT to_uuid_or_zero(value) FROM to_uuid_test;

DROP STREAM to_uuid_test;

