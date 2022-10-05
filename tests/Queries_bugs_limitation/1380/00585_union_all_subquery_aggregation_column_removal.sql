SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS clicks;
DROP STREAM IF EXISTS transactions;

create stream clicks (domain string) ;
create stream transactions (domain string) ;

INSERT INTO clicks(domain) VALUES ('facebook.com'), ('yandex.ru'), ('google.com');
INSERT INTO transactions(domain) VALUES ('facebook.com'), ('yandex.ru'), ('baidu.com');
SELECT sleep(3);

SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        count(*) AS total_count, 
        sum(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
FORMAT JSONEachRow;


SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
FORMAT JSONEachRow;


SELECT DISTINCT * FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10

UNION ALL

SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
);


SELECT DISTINCT total, domain FROM
(
SELECT
    sum(total_count) AS total, 
    sum(facebookHits) AS facebook,
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10

UNION ALL

SELECT
    sum(total_count) AS total, 
    max(facebookHits) AS facebook,
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
)
ORDER BY domain, total;


SELECT * FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js1
ALL FULL OUTER JOIN
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js2
USING (total, domain)
ORDER BY total, domain;


SELECT total FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js1
ALL FULL OUTER JOIN
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js2
USING (total, domain)
ORDER BY total, domain;


SELECT domain FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js1
ALL FULL OUTER JOIN
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        count(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        count(*) AS total_count, 
        to_uint64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js2
USING (total, domain)
ORDER BY total, domain;


DROP STREAM clicks;
DROP STREAM transactions;
