select to_decimal32(1.1, 2) as x group by x;
select to_decimal64(2.1, 4) as x group by x;
select to_decimal128(3.1, 12) as x group by x;

select materialize(to_decimal32(1.2, 2)) as x group by x;
select materialize(to_decimal64(2.2, 4)) as x group by x;
select materialize(to_decimal128(3.2, 12)) as x group by x;

select x from (select to_decimal32(1.3, 2) as x) group by x;
select x from (select to_decimal64(2.3, 4) as x) group by x;
select x from (select to_decimal128(3.3, 12) as x) group by x;

DROP STREAM IF EXISTS decimal;
create stream IF NOT EXISTS decimal
(
    A uint64,
    B Decimal128(18),
    C Decimal128(18)
) Engine = Memory;

INSERT INTO decimal(A,B,C) VALUES (1,1,1), (1,1,2), (1,1,3), (1,1,4);

SELECT A, to_string(B) AS B_str, to_string(sum(C)) AS c_str FROM decimal GROUP BY A, B_str;
SELECT A, B_str, to_string(cc) FROM (SELECT A, to_string(B) AS B_str, sum(C) AS cc FROM decimal GROUP BY A, B_str);

DROP STREAM decimal;
