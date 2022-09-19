SELECT UserID, array_enumerate_uniq(group_array(SearchPhrase)) AS arr
FROM
(
    SELECT UserID, SearchPhrase
    FROM test.hits
    WHERE CounterID = 1704509 AND UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE not_empty(SearchPhrase) AND CounterID = 1704509
        GROUP BY UserID
        HAVING count() > 1
    )
    ORDER BY UserID, WatchID
)
WHERE not_empty(SearchPhrase)
GROUP BY UserID
HAVING length(arr) > 1
ORDER BY UserID
LIMIT 20
