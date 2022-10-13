SELECT DISTINCT (split_by_string('/',URL)[3]) AS q, 'x' AS w from table(test.hits) WHERE CounterID = 14917930 ORDER BY URL

