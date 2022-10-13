SELECT GoalsReached AS k, count() AS c from table(test.hits) GROUP BY k ORDER BY c DESC LIMIT 10;
SELECT GeneralInterests AS k1, GoalsReached AS k2, count() AS c from table(test.hits) GROUP BY k1, k2 ORDER BY c DESC LIMIT 10;
SELECT ParsedParams.Key1 AS k1, GeneralInterests AS k2, count() AS c from table(test.hits) GROUP BY k1, k2 ORDER BY c DESC LIMIT 10;
SELECT ParsedParams.Key1 AS k1, GeneralInterests AS k2, count() AS c from table(test.hits) WHERE not_empty(k1) AND not_empty(k2) GROUP BY k1, k2 ORDER BY c DESC LIMIT 10;
