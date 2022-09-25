SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS memory;
create stream memory (x uint8) ;

INSERT INTO memory(x) VALUES (1);
INSERT INTO memory (x) VALUES (2);
INSERT INTO memory ( x) VALUES (3);
INSERT INTO memory (x ) VALUES (4);
INSERT INTO memory ( x ) VALUES (5);
INSERT INTO memory(x)VALUES(6);

SELECT sleep(3);

SELECT x FROM memory ORDER BY x;

SELECT sleep(2);

DROP STREAM memory;
