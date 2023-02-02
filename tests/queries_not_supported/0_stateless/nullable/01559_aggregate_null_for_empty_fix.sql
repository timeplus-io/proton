SELECT max(aggr)
FROM
(
	SELECT max(-1) AS aggr
	FROM system.one
	WHERE NOT 1
	UNION ALL
	SELECT max(-1) AS aggr
	FROM system.one
	WHERE 1

);
SELECT max(aggr)
FROM
(
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT max(aggr)
FROM
(
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT max(aggr)
FROM
(
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE not 1
);
SET aggregate_functions_null_for_empty=1;
SELECT max(aggr)
FROM
(
	SELECT max(-1) AS aggr
	FROM system.one
	WHERE NOT 1
	UNION ALL
	SELECT max(-1) AS aggr
	FROM system.one
	WHERE 1

);
SELECT max(aggr)
FROM
(
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT max(aggr)
FROM
(
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE 1
);
SELECT max(aggr)
FROM
(
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE NOT 1
    UNION ALL
    SELECT max(-1) AS aggr
    FROM system.one
    WHERE not 1
);
