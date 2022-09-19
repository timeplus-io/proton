SET query_mode = 'table';
drop stream IF EXISTS set;
create stream set (x string) ;
INSERT INTO set VALUES ('hello');
SELECT (array_join(['hello', 'world']) AS s) IN set, s;

drop stream set;
create stream set (x string) ENGINE = Set;
INSERT INTO set VALUES ('hello');
SELECT (array_join(['hello', 'world']) AS s) IN set, s;

drop stream set;

drop stream IF EXISTS join;
create stream join (k uint8, x string) ;
INSERT INTO join VALUES (1, 'hello');
SELECT k, x FROM (SELECT array_join([1, 2]) AS k) js1 ANY LEFT JOIN join USING k;

drop stream join;
create stream join (k uint8, x string) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join VALUES (1, 'hello');
SELECT k, x FROM (SELECT array_join([1, 2]) AS k) js1 ANY LEFT JOIN join USING k;

drop stream join;
