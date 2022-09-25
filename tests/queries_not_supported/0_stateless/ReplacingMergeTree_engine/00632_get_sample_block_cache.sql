SET joined_subquery_requires_alias = 0;

-- This test (SELECT) without cache can take tens minutes
DROP STREAM IF EXISTS dict_string;
DROP STREAM IF EXISTS dict_ui64;
DROP STREAM IF EXISTS video_views;

create stream video_views
(
    entityIri string,
    courseId uint64,
    learnerId uint64,
    actorId uint64,
    duration uint16,
    fullWatched uint8,
    fullWatchedDate datetime,
    fullWatchedDuration uint16,
    fullWatchedTime uint16,
    fullWatchedViews uint16,
    `views.viewId` array(string),
    `views.startedAt` array(datetime),
    `views.endedAt` array(datetime),
    `views.viewDuration` array(uint16),
    `views.watchedPart` array(Float32),
    `views.fullWatched` array(uint8),
    `views.progress` array(Float32),
    `views.reject` array(uint8),
    `views.viewNumber` array(uint16),
    `views.repeatingView` array(uint8),
    `views.ranges` array(string),
    version datetime
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY entityIri
ORDER BY (learnerId, entityIri)
SETTINGS index_granularity = 8192;

create stream dict_string (entityIri string) ;
create stream dict_ui64 (learnerId uint64) ;

--SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count`, `time-before-full-watched-average`, if (is_nan((`overall-full-watched-learners-count`/`overall-watchers-count`) * 100), 0, (`overall-full-watched-learners-count`/`overall-watchers-count`) * 100) as `overall-watched-part`, if (is_nan((`full-watched-learners-count`/`watchers-count` * 100)), 0, (`full-watched-learners-count`/`watchers-count` * 100)) as `full-watched-part`, if (is_nan((`rejects-count`/`views-count` * 100)), 0, (`rejects-count`/`views-count` * 100)) as `rejects-part` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count`, `time-before-full-watched-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average` FROM (SELECT `entityIri`, `watchers-count` FROM (SELECT `entityIri` FROM `CloM8CwMR2`) ANY LEFT JOIN (SELECT uniq(learnerId) as `watchers-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(viewDurationSum) as `time-repeating-average`, `entityIri` FROM (SELECT sum(views.viewDuration) as viewDurationSum, `entityIri`, `learnerId` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `views`.`repeatingView` = 1 AND `learnerId` IN `tkRpHxGqM1` GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.viewDuration) as `reject-views-duration-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `views`.`reject` = 1 AND `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(viewsCount) as `repeating-views-count-average`, `entityIri` FROM (SELECT count() as viewsCount, `learnerId`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `courseId` = 1 AND `entityIri` IN `CloM8CwMR2` WHERE `views`.`repeatingView` = 1 AND `learnerId` IN `tkRpHxGqM1` GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.viewDuration) as `views-duration-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.watchedPart) as `watched-part-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT count() as `rejects-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `views`.`reject` = 1 AND `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(progressMax) as `progress-average`, `entityIri` FROM (SELECT max(views.progress) as progressMax, `entityIri`, `learnerId` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(fullWatchedViews) as `views-count-before-full-watched-average`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT any(duration) as `duration`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `full-watched-learners-count`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `fullWatched` = 1 AND `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `overall-watchers-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `overall-full-watched-learners-count`,  `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `fullWatched` = 1 AND `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT count() as `views-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(fullWatchedTime) as `time-before-full-watched-average`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN `CloM8CwMR2` AND `courseId` = 1 WHERE `learnerId` IN `tkRpHxGqM1` GROUP BY `entityIri`) USING `entityIri`) FORMAT JSON;

SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count`, `time-before-full-watched-average`, if (is_nan((`overall-full-watched-learners-count`/`overall-watchers-count`) * 100), 0, (`overall-full-watched-learners-count`/`overall-watchers-count`) * 100) as `overall-watched-part`, if (is_nan((`full-watched-learners-count`/`watchers-count` * 100)), 0, (`full-watched-learners-count`/`watchers-count` * 100)) as `full-watched-part`, if (is_nan((`rejects-count`/`views-count` * 100)), 0, (`rejects-count`/`views-count` * 100)) as `rejects-part` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count`, `time-before-full-watched-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`,
 `reject-views-duration-average`, `repeating-views-count-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average` FROM (SELECT `entityIri`, `watchers-count` FROM (SELECT `entityIri` FROM dict_string) ANY LEFT JOIN (SELECT uniq(learnerId) as `watchers-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(viewDurationSum) as `time-repeating-average`, `entityIri` FROM (SELECT sum(views.viewDuration) as viewDurationSum, `entityIri`, `learnerId` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `views`.`repeatingView` = 1 AND `learnerId` IN dict_ui64 GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.viewDuration) as `reject-views-duration-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `views`.`reject` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(viewsCount) as `repeating-views-count-average`, `entityIri` FROM (SELECT count() as viewsCount, `learnerId`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `courseId` = 1 AND `entityIri` IN dict_string WHERE `views`.`repeatingView` = 1 AND `learnerId` IN dict_ui64 GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.viewDuration) as `views-duration-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.watchedPart) as `watched-part-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT count() as `rejects-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `views`.`reject` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(progressMax) as `progress-average`, `entityIri` FROM (SELECT max(views.progress) as progressMax, `entityIri`, `learnerId` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(fullWatchedViews) as `views-count-before-full-watched-average`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT any(duration) as `duration`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `full-watched-learners-count`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `fullWatched` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `overall-watchers-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `overall-full-watched-learners-count`,
  `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `fullWatched` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT count() as `views-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(fullWatchedTime) as `time-before-full-watched-average`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`);

SELECT 'Still alive';

DROP STREAM dict_string;
DROP STREAM dict_ui64;
DROP STREAM video_views;



-- Test for tsan: Ensure cache used from one thread
SET max_threads = 32;

DROP STREAM IF EXISTS sample_00632;

create stream sample_00632 (d date DEFAULT '2000-01-01', x uint16) ENGINE = MergeTree(d, x, x, 10);
INSERT INTO sample_00632 (x) SELECT to_uint16(number) AS x FROM system.numbers LIMIT 65536;

SELECT count()
FROM
(
    SELECT
        x,
        count() AS c
    FROM
    (
                  SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    )
    GROUP BY x
    --HAVING c = 1
    ORDER BY x ASC
);
DROP STREAM sample_00632;
