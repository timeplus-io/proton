SELECT array_join([3, 1, 2]) SETTINGS extremes = 1;
SELECT array_join([nan, 1, 2]) SETTINGS extremes = 1;
SELECT array_join([3, nan, 2]) SETTINGS extremes = 1;
SELECT array_join([3, 1, nan]) SETTINGS extremes = 1;
SELECT array_join([nan, nan, 2]) SETTINGS extremes = 1;
SELECT array_join([nan, 1, nan]) SETTINGS extremes = 1;
SELECT array_join([3, nan, nan]) SETTINGS extremes = 1;
SELECT array_join([nan, nan, nan]) SETTINGS extremes = 1;
