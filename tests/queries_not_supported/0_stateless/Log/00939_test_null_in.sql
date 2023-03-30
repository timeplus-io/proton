DROP STREAM IF EXISTS nullt;

CREATE STREAM nullt (c1 nullable(uint32), c2 nullable(string))ENGINE = Log;
INSERT INTO nullt VALUES (1, 'abc'), (2, NULL), (NULL, NULL);

SELECT c2 = ('abc') FROM nullt;
SELECT c2 IN ('abc') FROM nullt;

SELECT c2 IN ('abc', NULL) FROM nullt;

DROP STREAM IF EXISTS nullt;
