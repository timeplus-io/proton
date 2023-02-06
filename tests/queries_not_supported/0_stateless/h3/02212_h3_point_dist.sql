-- Tags: no-fasttest

DROP STREAM IF EXISTS stream1;

CREATE STREAM stream1 (lat1 float64, lon1 float64, lat2 float64, lon2 float64) ENGINE = Memory;

INSERT INTO stream1 VALUES(-10.0 ,0.0, 10.0, 0.0);
INSERT INTO stream1 VALUES(-1, -1, 2, 1);
INSERT INTO stream1 VALUES(0, 2, 1, 3);
INSERT INTO stream1 VALUES(-2, -3, -1, -2);
INSERT INTO stream1 VALUES(-87, 0, -85, 3);
INSERT INTO stream1 VALUES(-89, 1, -88, 2);
INSERT INTO stream1 VALUES(-84, 1, -83, 2);
INSERT INTO stream1 VALUES(-88, 90, -86, 91);
INSERT INTO stream1 VALUES(-84, -91, -83, -90);
INSERT INTO stream1 VALUES(-90, 181, -89, 182);
INSERT INTO stream1 VALUES(-84, 181, -83, 182);
INSERT INTO stream1 VALUES(-87, 0, -85, 3);

select '-- select h3PointDistM(lat1, lon1,lat2, lon2) AS k from stream1 order by k;';
select round(h3PointDistM(lat1, lon1,lat2, lon2), 2) AS k from stream1 order by k;
select '-- select h3PointDistKm(lat1, lon1,lat2, lon2) AS k from stream1 order by k;';
select round(h3PointDistKm(lat1, lon1,lat2, lon2), 2) AS k from stream1 order by k;
select '-- select h3PointDistRads(lat1, lon1,lat2, lon2) AS k from stream1 order by k;';
select round(h3PointDistRads(lat1, lon1,lat2, lon2), 5) AS k from stream1 order by k;

DROP STREAM stream1;

-- tests for const columns
select '-- test for non const cols';
select round(h3PointDistRads(-10.0 ,0.0, 10.0, array_join([0.0])), 5) as h3PointDistRads;
select round(h3PointDistRads(-10.0 ,0.0, 10.0, to_float64(0)) , 5)as h3PointDistRads;
