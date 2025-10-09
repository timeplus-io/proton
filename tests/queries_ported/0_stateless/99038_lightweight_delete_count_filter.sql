drop stream if exists 99038_sales;
select sleep(1) FORMAT Null;

CREATE STREAM 99038_sales(id uint32, product string, amount decimal(10, 2));
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;

INSERT INTO 99038_sales(id, product, amount) VALUES (1, 'Phone', 599.99),(2, 'Laptop', 1299.99),(3, 'Tablet', 399.99);
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;

select '(append only) before delete:';
select count() from table(99038_sales);
explain select count() from table(99038_sales);

SET allow_experimental_lightweight_delete = true;
DELETE FROM 99038_sales WHERE id = 2;
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;
select sleep(2) FORMAT Null;


select '(append only) after delete:';
select count() from table(99038_sales);
explain select count() from table(99038_sales);

drop stream if exists 99038_ss;

CREATE STREAM 99038_ss(id uint32, product string, amount decimal(10, 2)) engine = MergeTree ORDER BY id;
select sleep(2) FORMAT Null;

INSERT INTO 99038_ss(id, product, amount) VALUES (1, 'Phone', 599.99),(2, 'Laptop', 1299.99),(3, 'Tablet', 399.99);
select sleep(2) FORMAT Null;

select '(mergetree) before delete:';
select count() from 99038_ss;
explain select count() from 99038_ss;

SET allow_experimental_lightweight_delete = true;
DELETE FROM 99038_ss WHERE id = 2;

select sleep(2) FORMAT Null;
select '(mergetree) after delete:';
select count() from 99038_ss;
explain select count() from 99038_ss;
