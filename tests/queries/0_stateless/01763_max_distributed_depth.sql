-- Tags: distributed

DROP STREAM IF EXISTS tt6;

create stream tt6
(
	`id` uint32,
	`first_column` uint32,
	`second_column` uint32,
	`third_column` uint32,
	`status` string

)
ENGINE = Distributed('test_shard_localhost', '', 'tt7', rand());

create stream tt7 as tt6 ENGINE = Distributed('test_shard_localhost', '', 'tt6', rand());

INSERT INTO tt6 VALUES (1, 1, 1, 1, 'ok'); -- { serverError 581 }

SELECT * FROM tt6; -- { serverError 581 }

SET max_distributed_depth = 0;

-- stack overflow
INSERT INTO tt6 VALUES (1, 1, 1, 1, 'ok'); -- { serverError 306}

-- stack overflow
SELECT * FROM tt6; -- { serverError 306 }

DROP STREAM tt6;
