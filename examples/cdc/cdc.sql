-- read the topic via an external stream
CREATE EXTERNAL STREAM customers_cdc(raw string)
                SETTINGS type='kafka',
                         brokers='redpanda:9092',
                         topic='dbserver1.inventory.customers';

CREATE STREAM customers(id int, first_name string, last_name string,email string) 
PRIMARY KEY id 
SETTINGS mode='changelog_kv', version_column='_tp_time';          

CREATE MATERIALIZED VIEW mv_customers_r INTO customers AS 
    SELECT to_time(raw:payload.ts_ms) AS _tp_time, 
           raw:payload.after.id::int AS id,
           raw:payload.after.first_name AS first_name, 
           raw:payload.after.last_name AS last_name, 
           raw:payload.after.email AS email, 
           1::int8 as _tp_delta
    FROM customers_cdc WHERE raw:payload.op='r' SETTINGS seek_to='earliest';  
CREATE MATERIALIZED VIEW mv_customers_u INTO customers AS 
    WITH before AS(
        SELECT to_time(raw:payload.ts_ms)-1ms AS _tp_time, 
           raw:payload.before.id::int AS id,
           raw:payload.before.first_name AS first_name, 
           raw:payload.before.last_name AS last_name, 
           raw:payload.before.email AS email, 
           -1::int8 as _tp_delta
        FROM customers_cdc WHERE raw:payload.op='u' SETTINGS seek_to='earliest'
    ),after AS(
        SELECT to_time(raw:payload.ts_ms) AS _tp_time, 
           raw:payload.after.id::int AS id,
           raw:payload.after.first_name AS first_name, 
           raw:payload.after.last_name AS last_name, 
           raw:payload.after.email AS email, 
           1::int8 as _tp_delta
    FROM customers_cdc WHERE raw:payload.op='u' SETTINGS seek_to='earliest'
    )SELECT * FROM before UNION SELECT * FROM after;

CREATE MATERIALIZED VIEW mv_customers_d INTO customers AS 
    SELECT to_time(raw:payload.ts_ms) AS _tp_time, 
           raw:payload.before.id::int AS id,
           raw:payload.before.first_name AS first_name, 
           raw:payload.before.last_name AS last_name, 
           raw:payload.before.email AS email, 
           -1::int8 as _tp_delta
    FROM customers_cdc WHERE raw:payload.op='d' SETTINGS seek_to='earliest';      

CREATE MATERIALIZED VIEW mv_customers_c INTO customers AS 
    SELECT to_time(raw:payload.ts_ms) AS _tp_time, 
           raw:payload.after.id::int AS id,
           raw:payload.after.first_name AS first_name, 
           raw:payload.after.last_name AS last_name, 
           raw:payload.after.email AS email, 
           1::int8 as _tp_delta
    FROM customers_cdc WHERE raw:payload.op='c' SETTINGS seek_to='earliest';   