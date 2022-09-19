DROP STREAM IF EXISTS a;
DROP STREAM IF EXISTS b;
DROP STREAM IF EXISTS id1;
DROP STREAM IF EXISTS id2;

create stream a(`id1` uint32, `id2` uint32, `valA` uint32) ;
create stream id1(`id1` uint32, `val1` uint8) ENGINE = Join(ANY, LEFT, id1);
create stream id2(`id2` uint32, `val2` uint8) ENGINE = Join(ANY, LEFT, id2);

INSERT INTO a VALUES (1,1,1)(2,2,2)(3,3,3); 
INSERT INTO id1 VALUES (1,1)(2,2)(3,3);
INSERT INTO id2 VALUES (1,1)(2,2)(3,3);

SELECT * from (SELECT * FROM a ANY LEFT OUTER JOIN id1 USING id1) js1 ANY LEFT OUTER JOIN id2 USING id2;

create view b as (SELECT * from (SELECT * FROM a ANY LEFT OUTER JOIN id1 USING id1) js1 ANY LEFT OUTER JOIN id2 USING id2);
SELECT '-';
SELECT * FROM b;

DROP STREAM a;
DROP STREAM b;
DROP STREAM id1;
DROP STREAM id2;
