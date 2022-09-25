SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;

DROP STREAM IF EXISTS local_statements;
DROP STREAM IF EXISTS statements;

create stream local_statements ( statementId string, eventDate date, eventHour DateTime, eventTime DateTime, verb string, objectId string, onCourse uint8, courseId UInt16, contextRegistration string, resultScoreRaw float64, resultScoreMin float64, resultScoreMax float64, resultSuccess uint8, resultCompletition uint8, resultDuration uint32, resultResponse string, learnerId string, learnerHash string, contextId UInt16) ENGINE = MergeTree ORDER BY tuple();

create stream statements ( statementId string, eventDate date, eventHour DateTime, eventTime DateTime, verb string, objectId string, onCourse uint8, courseId UInt16, contextRegistration string, resultScoreRaw float64, resultScoreMin float64, resultScoreMax float64, resultSuccess uint8, resultCompletition uint8, resultDuration uint32, resultResponse string, learnerId string, learnerHash string, contextId UInt16) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'local_statements', sipHash64(learnerHash));

INSERT INTO local_statements FORMAT CSV "2b3b04ee-0bb8-4200-906f-d47c48e56bd0","2016-08-25","2016-08-25 14:00:00","2016-08-25 14:43:34","http://adlnet.gov/expapi/verbs/passed","https://crmm.ru/xapi/courses/spp/2/0/3/2/8",0,1,"c13d788c-26e0-40e3-bacb-a1ff78ee1518",100,0,0,0,0,0,"","https://sberbank-school.ru/xapi/accounts/userid/94312","6f696f938a69b5e173093718e1c2bbf2",0

SELECT avg(diff)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            learnerHash,
            passed - eventTime AS diff
        FROM statements
        GLOBAL SEMI LEFT JOIN
        (
            SELECT
                learnerHash,
                arg_max(eventTime, resultScoreRaw) AS passed
            FROM
            (
                SELECT
                    learnerHash,
                    eventTime,
                    resultScoreRaw
                FROM statements
                WHERE (courseId = 1) AND (onCourse = 0)
                    AND (verb = 'http://adlnet.gov/expapi/verbs/passed') AND (objectId = 'https://crmm.ru/xapi/courses/spp/1/1/0-1')
                ORDER BY eventTime ASC
            )
            GROUP BY learnerHash
        ) USING (learnerHash)
        WHERE (courseId = 1) AND (onCourse = 0)
            AND (verb = 'http://adlnet.gov/expapi/verbs/interacted') AND (eventTime <= passed) AND (diff > 0)
        ORDER BY eventTime DESC
        LIMIT 1 BY learnerHash
    )
    ORDER BY diff DESC
    LIMIT 7, 126
);

DROP STREAM local_statements;
DROP STREAM statements;
