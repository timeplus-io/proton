DROP STREAM IF EXISTS products;

SET allow_experimental_analyzer = 1;

CREATE STREAM products (`price` uint32) ENGINE = Memory;
INSERT INTO products VALUES (1);

SELECT rank() OVER (ORDER BY price) AS rank FROM products ORDER BY rank;
