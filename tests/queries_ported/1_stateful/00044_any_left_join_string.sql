SELECT
    domain,
    hits,
    visits
FROM
(
    SELECT
        split_by_string('/', URL)[3] AS domain,
        count() AS hits
    from table(test.hits)
    GROUP BY domain
) ANY LEFT JOIN
(
    SELECT
        split_by_string('/', StartURL)[3] AS domain,
        sum(Sign) AS visits
    FROM table(test.visits)
    GROUP BY domain
) USING domain
ORDER BY hits DESC
LIMIT 10
SETTINGS joined_subquery_requires_alias = 0;
