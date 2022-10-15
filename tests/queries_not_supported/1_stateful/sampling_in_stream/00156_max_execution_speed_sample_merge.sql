SET max_execution_speed = 4000000, timeout_before_checking_execution_speed = 0.001;

CREATE TEMPORARY STREAM times (t DateTime) engine = Memory;

INSERT INTO times SELECT now();
SELECT count() FROM table(test.hits) SAMPLE 1 / 2;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT count() FROM merge(test, '^hits$') SAMPLE 1 / 2;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
