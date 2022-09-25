SET query_mode = 'table';


drop stream IF EXISTS join;
create stream join (k uint8, x string) ;
INSERT INTO join(k,x) VALUES (1, 'hello');
SELECT sleep(3);
SELECT k, x FROM (SELECT array_join([1, 2]) AS k) js1 ANY LEFT JOIN join USING k;

drop stream join;
create stream join (k uint8, x string) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join(k,x) VALUES (1, 'hello');
SELECT sleep(3);
SELECT k, x FROM (SELECT array_join([1, 2]) AS k) js1 ANY LEFT JOIN join USING k;

drop stream join;
