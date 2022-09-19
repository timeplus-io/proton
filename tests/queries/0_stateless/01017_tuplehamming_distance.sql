SELECT tupleHammingDistance((1, 2), (3, 4));
SELECT tupleHammingDistance((120, 243), (120, 434));
SELECT tupleHammingDistance((-12, 434), (434, 434));

DROP STREAM IF EXISTS defaults;
create stream defaults
(
	t1 tuple(uint16, uint16),
	t2 tuple(uint32, uint32),
	t3 tuple(int64, int64)
)();

INSERT INTO defaults VALUES ((12, 43), (12312, 43453) ,(-10, 32)) ((1, 4), (546, 12345), (546, 12345)) ((90, 9875), (43456, 234203), (1231, -123)) ((87, 987), (545645, 768354634), (9123, 909));

SELECT tupleHammingDistance((12, 43), t1) FROM defaults;
SELECT tupleHammingDistance(t2, (546, 456)) FROM defaults;
SELECT tupleHammingDistance(t2, t3) FROM defaults;

DROP STREAM defaults;
