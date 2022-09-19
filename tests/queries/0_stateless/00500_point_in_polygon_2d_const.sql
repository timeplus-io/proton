
SET query_mode = 'table';
SELECT pointInPolygon((0, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)]]);

drop stream IF EXISTS s;
create stream s (`id` string, `lng` int64, `lat` int64) ();

drop stream IF EXISTS p;
create stream p (`polygon_id` int64, `polygon_name` string, `shape` array(array(tuple(float64, float64))), `state` string) ();

INSERT INTO s VALUES ('a', 0, 0);
INSERT INTO p VALUES (8, 'a', [[(0, 0), (10, 0), (10, 10), (0, 10)]], 'a');
SELECT id FROM s WHERE pointInPolygon((lng,lat), (select shape from p where polygon_id = 8));

drop stream s;
drop stream p;
