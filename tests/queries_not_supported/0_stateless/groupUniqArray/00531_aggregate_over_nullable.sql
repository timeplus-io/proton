DROP STREAM IF EXISTS agg_over_nullable;
create stream agg_over_nullable (
	partition date,
	timestamp datetime,
	user_id nullable(uint32),
	description nullable(string)
) ENGINE = MergeTree(partition, timestamp, 8192);

INSERT INTO agg_over_nullable(partition, timestamp, user_id, description) VALUES(now(), now(), 1, 'ss');
INSERT INTO agg_over_nullable(partition, timestamp, user_id, description) VALUES(now(), now(), 1, NULL);
INSERT INTO agg_over_nullable(partition, timestamp, user_id, description) VALUES(now(), now(), 1, 'aa');

SELECT arraySort(groupUniqArray(description)) FROM agg_over_nullable;
SELECT arraySort(topK(3)(description)) FROM agg_over_nullable;

DROP STREAM agg_over_nullable;
