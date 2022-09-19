-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01084;
CREATE DATABASE test_01084;
USE test_01084;
create stream t (x uint8) ;

SELECT * FROM merge('', '');

DROP DATABASE test_01084;
