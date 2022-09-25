-- Tags: distributed

-- test from https://github.com/ClickHouse/ClickHouse/issues/11755#issuecomment-700850254
DROP STREAM IF EXISTS cat_hist;
DROP STREAM IF EXISTS prod_hist;
DROP STREAM IF EXISTS products_l;
DROP STREAM IF EXISTS products;

create stream cat_hist (categoryId uuid, categoryName string) ENGINE Memory;
create stream prod_hist (categoryId uuid, productId uuid) ENGINE = MergeTree ORDER BY productId;

create stream products_l AS prod_hist ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), prod_hist);
create stream products as prod_hist ENGINE = Merge(currentDatabase(), '^products_');

SELECT * FROM products AS p LEFT JOIN cat_hist AS c USING (categoryId);
SELECT * FROM products AS p GLOBAL LEFT JOIN cat_hist AS c USING (categoryId);

DROP STREAM cat_hist;
DROP STREAM prod_hist;
DROP STREAM products_l;
DROP STREAM products;
