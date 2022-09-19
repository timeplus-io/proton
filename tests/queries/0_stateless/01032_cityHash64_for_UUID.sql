SELECT cityHash64(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) AS uuid;
DROP STREAM IF EXISTS t_uuid;
create stream t_uuid (x UUID) ;
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
SELECT (SELECT count() FROM t_uuid WHERE cityHash64(reinterpret_as_string(x)) = cityHash64(x) and length(reinterpret_as_string(x)) = 16) = (SELECT count() AS c2 FROM t_uuid WHERE length(reinterpret_as_string(x)) = 16);
DROP STREAM IF EXISTS t_uuid;
