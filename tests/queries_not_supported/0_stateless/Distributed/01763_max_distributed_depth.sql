-- Tags: distributed

SET prefer_localhost_replica = 1;

DROP STREAM IF EXISTS tt6;

CREATE STREAM tt6
(
	`id` uint32,
	`first_column` uint32,
	`second_column` uint32,
	`third_column` uint32,
	`status` string

)
ENGINE = Distributed('test_shard_localhost', '', 'tt7', rand());

DROP STREAM IF EXISTS tt7;

CREATE STREAM tt7 as tt6 ENGINE = Distributed('test_shard_localhost', '', 'tt6', rand());

INSERT INTO tt6 VALUES (1, 1, 1, 1, 'ok'); -- { serverError 581 }

SELECT * FROM tt6; -- { serverError 581 }

SET max_distributed_depth = 0;

-- stack overflow
INSERT INTO tt6 VALUES (1, 1, 1, 1, 'ok'); -- { serverError 306}

-- stack overflow
SELECT * FROM tt6; -- { serverError 306 }

DROP STREAM tt6;
DROP STREAM tt7;
