DROP STREAM IF EXISTS truncate_test;

create stream truncate_test(uint8 uint8)  ;

INSERT INTO truncate_test VALUES(1), (2), (3);

SELECT * FROM truncate_test ORDER BY uint8;

TRUNCATE truncate_test;

SELECT * FROM truncate_test ORDER BY uint8;

DROP STREAM truncate_test;
