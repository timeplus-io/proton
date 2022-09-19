DROP STREAM IF EXISTS nullt;

create stream nullt (c1 Nullable(uint32), c2 Nullable(string)) ;
INSERT INTO nullt VALUES (1, 'abc'), (2, NULL), (NULL, NULL);

SELECT c2 = ('abc') FROM nullt;
SELECT c2 IN ('abc') FROM nullt;

SELECT c2 IN ('abc', NULL) FROM nullt;

DROP STREAM IF EXISTS nullt;
