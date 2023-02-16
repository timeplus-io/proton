-- Tags: distributed

DROP STREAM IF EXISTS d_numbers;
CREATE STREAM d_numbers (number uint32) ENGINE = Distributed(test_cluster_two_shards, system, numbers, rand());

SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2;

SET distributed_product_mode = 'local';

SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2;

DROP STREAM d_numbers;
