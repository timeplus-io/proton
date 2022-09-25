SELECT round(greatCircleAngle(0, 45, 0.1, 45.1), 4);
SELECT round(greatCircleAngle(0, 45, 1, 45), 4);
SELECT round(greatCircleAngle(0, 45, 1, 45.1), 4);

SELECT round(great_circle_distance(0, 0, 0, 90), 4);
SELECT round(great_circle_distance(0, 0, 90, 0), 4);
