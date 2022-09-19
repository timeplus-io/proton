SET query_mode = 'table';
drop stream IF EXISTS A;
drop stream IF EXISTS B;

create stream A(k uint32, t uint32, a uint64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (1,101,1),(1,102,2),(1,103,3),(1,104,4),(1,105,5);

create stream B(k uint32, t uint32, b uint64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,102,2), (1,104,4);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);
drop stream B;


create stream B(t uint32, k uint32, b uint64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,102,2), (1,104,4);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);
drop stream B;

create stream B(k uint32, b uint64, t uint32) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,102,2), (1,104,4);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);
drop stream B;

drop stream A;
