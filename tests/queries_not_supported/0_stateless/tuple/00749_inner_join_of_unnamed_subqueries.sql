set joined_subquery_requires_alias = 0;

DROP STREAM IF EXISTS left_table;
DROP STREAM IF EXISTS right_table;

create stream left_table(APIKey int32, SomeColumn string) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left_table VALUES(1, 'somestr');

create stream right_table(APIKey int32, EventValueForPostback string) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO right_table VALUES(1, 'hello'), (2, 'WORLD');

SELECT
    APIKey,
    ConversionEventValue
FROM
    left_table AS left_table
ALL INNER JOIN
    (
        SELECT *
        FROM
            (
                SELECT
                    APIKey,
                    EventValueForPostback AS ConversionEventValue
                FROM
                    right_table AS right_table
            )
            ALL INNER JOIN
            (
                SELECT
                    APIKey
                FROM
                    left_table as left_table
                GROUP BY
                    APIKey
            ) USING (APIKey)
    ) USING (APIKey);

DROP STREAM IF EXISTS left_table;
DROP STREAM IF EXISTS right_table;
