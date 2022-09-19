-- Tags: distributed

DROP STREAM IF EXISTS distr0;
DROP STREAM IF EXISTS distr1;
DROP STREAM IF EXISTS distr2;

create stream distr (x uint8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr); -- { serverError 269 }

create stream distr0 (x uint8) ENGINE = Distributed(test_shard_localhost, '', distr0); -- { serverError 269 }

create stream distr1 (x uint8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr2);
create stream distr2 (x uint8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr1);

SELECT * FROM distr1; -- { serverError 581 }
SELECT * FROM distr2; -- { serverError 581 }

DROP STREAM distr1;
DROP STREAM distr2;
