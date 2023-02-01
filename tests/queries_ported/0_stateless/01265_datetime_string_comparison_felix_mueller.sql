DROP STREAM IF EXISTS tztest;

CREATE STREAM tztest
(
    timeBerlin DateTime('Europe/Berlin'), 
    timeLA DateTime('America/Los_Angeles')
)
ENGINE = Memory;

INSERT INTO tztest (timeBerlin, timeLA) VALUES ('2019-05-06 12:00:00', '2019-05-06 12:00:00');

SELECT
    to_unix_timestamp(timeBerlin),
    to_unix_timestamp(timeLA)
FROM tztest;

SELECT 1
FROM tztest
WHERE timeBerlin = '2019-05-06 12:00:00';

SELECT 1
FROM tztest
WHERE timeLA = '2019-05-06 12:00:00';

SELECT 1
FROM tztest
WHERE '2019-05-06 12:00:00' = timeBerlin;

DROP STREAM tztest;
