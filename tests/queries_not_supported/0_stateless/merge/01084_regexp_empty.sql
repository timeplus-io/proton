-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01084;
CREATE DATABASE test_01084;
USE test_01084;
CREATE STREAM t (x uint8) ENGINE = Memory;

SELECT * FROM merge('', '');

DROP DATABASE test_01084;
