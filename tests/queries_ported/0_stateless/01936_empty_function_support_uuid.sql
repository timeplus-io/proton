SELECT empty(to_uuid('00000000-0000-0000-0000-000000000000'));
SELECT not_empty(to_uuid('00000000-0000-0000-0000-000000000000'));
SELECT uniq_if(uuid, empty(uuid)), uniq_if(uuid, not_empty(uuid))
FROM
(
    SELECT to_uuid('00000000-0000-0000-0000-000000000002') AS uuid
    UNION ALL
    SELECT to_uuid('00000000-0000-0000-0000-000000000000') AS uuid
    UNION ALL
    SELECT to_uuid('00000000-0000-0000-0000-000000000001') AS uuid
);

DROP STREAM IF EXISTS users;
DROP STREAM IF EXISTS orders;

CREATE STREAM users (user_id uuid) ENGINE = Memory;
CREATE STREAM orders (order_id uuid, user_id uuid) ENGINE = Memory;

INSERT INTO users VALUES ('00000000-0000-0000-0000-000000000001');
INSERT INTO users VALUES ('00000000-0000-0000-0000-000000000002');
INSERT INTO orders VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001');

SELECT
    uniq(user_id) AS users,
    uniq_if(order_id, not_empty(order_id)) AS orders
FROM
(
    SELECT * FROM users
) as t1 ALL LEFT JOIN (
    SELECT * FROM orders
) as t2 USING (user_id);

DROP STREAM users;
DROP STREAM orders;

