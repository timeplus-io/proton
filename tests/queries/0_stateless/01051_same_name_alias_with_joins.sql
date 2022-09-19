DROP STREAM IF EXISTS a;
DROP STREAM IF EXISTS b;
DROP STREAM IF EXISTS c;

create stream a (x uint64) ;
create stream b (x uint64) ;
create stream c (x uint64) ;

SET enable_optimize_predicate_expression = 0;

SELECT a.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON a.x = c.x;

SELECT a.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.x = c.x;

SELECT b.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.x = c.x;

SELECT c.x AS x FROM a
LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.x = c.x;

DROP STREAM a;
DROP STREAM b;
DROP STREAM c;
