DROP STREAM IF EXISTS Dates;

CREATE STREAM Dates (date datetime('UTC')) ENGINE = MergeTree() ORDER BY date;

INSERT INTO Dates VALUES ('2023-08-25 15:30:00');

SELECT format_datetime((SELECT date FROM Dates), '%H%i%S', number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT format_datetime((SELECT materialize(date) FROM Dates), '%H%i%S', number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT format_datetime((SELECT materialize(date) FROM Dates), '%H%i%S', number % 2 ? '' : 'Europe/Amsterdam') FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT to_string((SELECT date FROM Dates), number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT to_string((SELECT materialize(date) FROM Dates), number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT to_string((SELECT materialize(date) FROM Dates), number % 2 ? 'America/Los_Angeles' : '') FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP STREAM Dates;
