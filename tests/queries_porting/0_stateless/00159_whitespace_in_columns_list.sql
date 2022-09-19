SET query_mode='table';
DROP STREAM IF EXISTS memory;
create stream memory (x uint8) ;

INSERT INTO memory(x) VALUES (1);
INSERT INTO memory (x) VALUES (2);
INSERT INTO memory ( x) VALUES (3);
INSERT INTO memory (x ) VALUES (4);
INSERT INTO memory ( x ) VALUES (5);
INSERT INTO memory(x)VALUES(6);

SELECT * FROM memory ORDER BY x;

DROP STREAM memory;
