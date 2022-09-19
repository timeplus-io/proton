DROP STREAM IF EXISTS float;
create stream float (x float64)  ;

INSERT INTO float VALUES (1e7);
SELECT * FROM float;

DROP STREAM float;
