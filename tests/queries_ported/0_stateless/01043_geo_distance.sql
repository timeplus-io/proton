SELECT great_circle_distance(0., 0., 0., 1.);
SELECT great_circle_distance(0., 89., 0, 90.);

SELECT geo_distance(0., 0., 0., 1.);
SELECT geo_distance(0., 89., 0., 90.);

SELECT great_circle_distance(0., 0., 90., 0.);
SELECT great_circle_distance(0., 0., 0., 90.);

SELECT geo_distance(0., 0., 90., 0.);
SELECT geo_distance(0., 0., 0., 90.);
