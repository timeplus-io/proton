SELECT geohashes_in_box(1., 2., 3., 4., 1);
SELECT geohashes_in_box(materialize(1.), 2., 3., 4., 2);
SELECT geohashes_in_box(1., materialize(2.), 3., 4., 3);
SELECT geohashes_in_box(1., 2., materialize(3.), 4., 1);
SELECT geohashes_in_box(1., 2., 3., materialize(4.), 2);
SELECT geohashes_in_box(1., 2., 3., 4., materialize(3));
