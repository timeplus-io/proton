SELECT SearchEngineID AS k1, SearchPhrase AS k2, count() AS c from table(test.hits) GROUP BY k1, k2 ORDER BY c DESC, k1, k2 LIMIT 10
