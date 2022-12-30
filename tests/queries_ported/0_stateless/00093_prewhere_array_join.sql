SELECT array_join([SearchEngineID]) AS search_engine, URL FROM test.hits WHERE SearchEngineID != 0 AND search_engine != 0 FORMAT Null;

SELECT
    array_join([0]) AS browser,
    array_join([SearchEngineID]) AS search_engine,
    URL
FROM test.hits
WHERE 1 AND (SearchEngineID != 0) AND (browser != 0) AND (search_engine != 0)
FORMAT Null;
