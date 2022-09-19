CREATE TEMPORARY STREAM test (`id` string, `products` nested (`产品` array(string), `销量` array(int32)));

DESCRIBE test;
DESCRIBE (SELECT * FROM test);
DESCRIBE (SELECT * FROM test ARRAY JOIN products);
DESCRIBE (SELECT p.`产品`, p.`销量` FROM test ARRAY JOIN products AS p);
SELECT * FROM test ARRAY JOIN products;
SELECT count() FROM (SELECT * FROM test ARRAY JOIN products);
