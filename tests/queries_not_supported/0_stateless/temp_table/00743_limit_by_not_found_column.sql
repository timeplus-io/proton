DROP STREAM IF EXISTS installation_stats;
create stream installation_stats (message string, info string, message_type string)  ;

SELECT count(*) AS total
FROM
(
    SELECT
        message,
        info,
        count() AS cnt
    FROM installation_stats
    WHERE message_type LIKE 'fail'
    GROUP BY
        message,
        info
    ORDER BY cnt DESC
    LIMIT 5 BY message
);

DROP STREAM installation_stats;

CREATE TEMPORARY STREAM Accounts (AccountID uint64, Currency string);

SELECT AccountID
FROM
(
    SELECT
        AccountID, 
        Currency
    FROM Accounts 
    LIMIT 2 BY Currency
);

CREATE TEMPORARY STREAM commententry1 (created_date date, link_id string, subreddit string);
INSERT INTO commententry1 VALUES ('2016-01-01', 'xyz', 'cpp');

SELECT concat('http://reddit.com/r/', subreddit, '/comments/', replaceRegexpOne(link_id, 't[0-9]_', ''))
FROM
(
    SELECT
        y,
        subreddit,
        link_id,
        cnt
    FROM
    (
        SELECT
            created_date AS y,
            link_id,
            subreddit,
            count(*) AS cnt
        FROM commententry1
        WHERE toYear(created_date) = 2016
        GROUP BY
            y,
            link_id,
            subreddit
        ORDER BY y ASC
    )
    ORDER BY
        y ASC,
        cnt DESC
    LIMIT 1 BY y
);
