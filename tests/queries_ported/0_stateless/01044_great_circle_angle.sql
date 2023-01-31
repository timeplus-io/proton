WITH number - 90 AS lat SELECT DISTINCT great_circle_angle(0, 0, 0, lat) = abs(lat) FROM numbers(180);
WITH number - 180 AS lon SELECT lon, round(great_circle_angle(0, 0, lon, 0) - abs(lon) AS err, 2) FROM numbers(360) WHERE abs(err) > 0.01;
SELECT bar((great_circle_angle(0, 0, number, number) - number) * 100, 0, 2000, 100) FROM numbers(90);
