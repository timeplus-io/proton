DROP STREAM IF EXISTS A;
DROP STREAM IF EXISTS B;

create stream A(a uint32, t uint32) ;
create stream B(b uint32, t uint32) ;

INSERT INTO A (a,t) VALUES (1,1),(1,2),(1,3), (2,1),(2,2),(2,3), (3,1),(3,2),(3,3);
INSERT INTO B (b,t) VALUES (1,2),(1,4),(2,3);

SELECT A.a, A.t, B.b, B.t FROM A ASOF LEFT JOIN B ON A.a == B.b AND A.t >= B.t ORDER BY (A.a, A.t);
SELECT count() FROM A ASOF LEFT JOIN B ON A.a == B.b AND B.t <= A.t;
SELECT A.a, A.t, B.b, B.t FROM A ASOF INNER JOIN B ON B.t <= A.t AND A.a == B.b ORDER BY (A.a, A.t);
SELECT '-';
SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t <= B.t ORDER BY (A.a, A.t);
SELECT '-';
SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND B.t >= A.t ORDER BY (A.a, A.t);
SELECT '-';
SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t > B.t ORDER BY (A.a, A.t);
SELECT '-';
SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t < B.t ORDER BY (A.a, A.t);
SELECT count() FROM A ASOF JOIN B ON A.a == B.b AND A.t == B.t; -- { serverError 403 }
SELECT count() FROM A ASOF JOIN B ON A.a == B.b AND A.t != B.t; -- { serverError 403 }

SELECT A.a, A.t, B.b, B.t FROM A ASOF JOIN B ON A.a == B.b AND A.t < B.t OR A.a == B.b + 1 ORDER BY (A.a, A.t); -- { serverError 48 }

DROP STREAM A;
DROP STREAM B;
