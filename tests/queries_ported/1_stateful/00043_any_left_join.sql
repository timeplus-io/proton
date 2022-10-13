SELECT
    EventDate,
    count() AS hits,
    any(visits)
from table(test.hits) ANY LEFT JOIN
(
    SELECT
        StartDate AS EventDate,
        sum(Sign) AS visits
    FROM table(test.visits)
    GROUP BY EventDate
) USING EventDate
GROUP BY EventDate
ORDER BY hits DESC
LIMIT 10
SETTINGS joined_subquery_requires_alias = 0;
