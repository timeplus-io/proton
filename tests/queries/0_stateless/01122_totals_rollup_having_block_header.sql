DROP STREAM IF EXISTS rollup_having;
create stream rollup_having (
  a nullable(string),
  b nullable(string)
) ;

INSERT INTO rollup_having VALUES (NULL, NULL);
INSERT INTO rollup_having VALUES ('a', NULL);
INSERT INTO rollup_having VALUES ('a', 'b');

SELECT a, b, count(*) FROM rollup_having GROUP BY a, b WITH ROLLUP WITH TOTALS HAVING a IS NOT NULL; -- { serverError 48 }
SELECT a, b, count(*) FROM rollup_having GROUP BY a, b WITH ROLLUP WITH TOTALS HAVING a IS NOT NULL and b IS NOT NULL; -- { serverError 48 }

DROP STREAM rollup_having;
