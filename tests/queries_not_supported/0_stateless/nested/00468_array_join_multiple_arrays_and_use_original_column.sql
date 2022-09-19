DROP STREAM IF EXISTS nested;
create stream nested (nest nested(x uint8, y uint8)) ;
INSERT INTO nested VALUES ([1, 2, 3], [4, 5, 6]);

SELECT nx FROM nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE not_empty(nest.y);
SELECT 1 FROM nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE not_empty(nest.y);
SELECT nx, ny FROM nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE not_empty(nest.y);
SELECT nx FROM nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE not_empty(nest.x);
SELECT nx, nest.y FROM nested ARRAY JOIN nest.x AS nx, nest.y AS ny;
SELECT nx, ny, nest.x, nest.y FROM nested ARRAY JOIN nest.x AS nx, nest.y AS ny;

DROP STREAM nested;
